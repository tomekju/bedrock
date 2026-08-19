defmodule Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase do
  @moduledoc """
  Bootstraps the metadata shard materializer for recovery.

  The metadata materializer holds the authoritative shard layout - the mapping from
  key ranges to shard tags. This phase ensures the materializer is available and
  queries it for the current shard layout.

  ## Fresh Cluster

  For a fresh cluster (no old logs), creates a default shard layout with two shards:
  - System shard (tag 0): Keys from 0xFF to end-of-keyspace (system metadata)
  - User shard (tag 1): Keys from empty string to 0xFF (user data)

  ## Existing Cluster

  For an existing cluster:
  1. Find materializer with shard_id = 0 (system shard) in available_services
  2. If not found, create a new materializer on a capable node
  3. Lock materializer for recovery
  4. Unlock it with system shard logs to start pulling
  5. Wait for materializer to catch up (60s timeout)
  6. Query shard layout from `\\xff/system/shard_keys/*`

  Stalls if the materializer is unavailable and cannot be created, or if catchup
  times out. Transitions to CommitProxyStartupPhase with the materializer pid and
  shard layout.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [end_of_keyspace: 0]
  import Bedrock.ControlPlane.Config.ResolverDescriptor, only: [resolver_descriptor: 2]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  require Logger

  # Catchup timeout: 60 seconds before stalling and retrying
  @catchup_timeout_ms 60_000
  @catchup_poll_interval_ms 500

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    if fresh_cluster?(context) do
      handle_fresh_cluster(recovery_attempt, context)
    else
      handle_existing_cluster(recovery_attempt, context)
    end
  end

  @doc """
  Returns the default shard layout for a fresh cluster.

  Layout has two shards:
  - Tag 0: System keys (0xFF to end_of_keyspace)
  - Tag 1: User keys (empty string to 0xFF)

  The map is keyed by end_key, with values of {tag, start_key}.
  """
  @spec default_shard_layout() :: RecoveryAttempt.shard_layout()
  def default_shard_layout do
    %{
      # User shard: "" to 0xFF
      <<0xFF>> => {1, <<>>},
      # System shard: 0xFF to end
      end_of_keyspace() => {0, <<0xFF>>}
    }
  end

  # Private implementation

  defp fresh_cluster?(%{old_transaction_system_layout: nil}), do: true
  defp fresh_cluster?(%{old_transaction_system_layout: %{logs: logs}}) when map_size(logs) == 0, do: true
  defp fresh_cluster?(_context), do: false

  defp handle_fresh_cluster(recovery_attempt, context) do
    Logger.debug("Fresh cluster detected, using default shard layout")

    shard_layout = default_shard_layout()
    shard_tags = extract_shard_tags(shard_layout)

    # PATCHED (fuu): issue snapshotless tokens before fresh materializer startup only.
    with :ok <-
           issue_initial_snapshotless_tokens(
             recovery_attempt.cluster,
             recovery_attempt.epoch,
             shard_tags
           ),
         {:ok, shard_materializers, recovery_attempt} <-
           create_materializers_for_shards(shard_tags, recovery_attempt, context) do
      # Get the system shard materializer as metadata_materializer for backward compat
      system_shard = RecoveryAttempt.system_shard_id()
      metadata_materializer = Map.get(shard_materializers, system_shard)

      updated_attempt =
        recovery_attempt
        |> Map.put(:metadata_materializer, metadata_materializer)
        |> Map.put(:shard_layout, shard_layout)
        |> Map.put(:shard_materializers, shard_materializers)

      {updated_attempt, CommitProxyStartupPhase}
    else
      {:error, reason} ->
        Logger.warning("Failed to create materializers for fresh cluster: #{inspect(reason)}")
        {recovery_attempt, {:stalled, {:materializer_creation_failed, reason}}}
    end
  end

  # Extract unique shard tags from shard_layout
  defp extract_shard_tags(shard_layout) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {tag, _start_key} -> tag end)
    |> Enum.uniq()
  end

  # Create materializers for multiple shards
  defp create_materializers_for_shards(shard_tags, recovery_attempt, context) do
    Enum.reduce_while(shard_tags, {:ok, %{}, recovery_attempt}, fn shard_tag, {:ok, acc, recovery_attempt} ->
      case create_and_start_materializer(shard_tag, recovery_attempt, context) do
        {:ok, {service_id, worker_ref, node, pid}} ->
          # PATCHED (fuu): record materializer services in recovery topology.
          recovery_attempt =
            record_materializer_service(recovery_attempt, service_id, worker_ref, node, pid)

          {:cont, {:ok, Map.put(acc, shard_tag, pid), recovery_attempt}}

        {:error, reason} ->
          {:halt, {:error, {shard_tag, reason}}}
      end
    end)
  end

  # Create a materializer for a specific shard and start it pulling
  defp create_and_start_materializer(shard_tag, recovery_attempt, context) do
    with {:ok, node} <- find_materializer_capable_node(context),
         {:ok, {service_id, worker_ref, node}} <- create_materializer_worker(node, shard_tag, recovery_attempt, context),
         {:ok, pid} <-
           lock_new_materializer({:materializer, {worker_ref, node}, shard_tag}, recovery_attempt.epoch, context),
         :ok <- start_materializer_pulling(pid, shard_tag, recovery_attempt, context) do
      {:ok, {service_id, worker_ref, node, pid}}
    end
  end

  # Create worker via Foreman for a specific shard
  defp create_materializer_worker(node, shard_tag, recovery_attempt, context) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()
    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    case create_worker_fn.(foreman_ref, worker_id, :materializer,
           timeout: 30_000,
           params: materializer_params(shard_tag)
         ) do
      {:ok, worker_ref} -> {:ok, {worker_id, worker_ref, node}}
      {:error, reason} -> {:error, {:failed_to_create_materializer, reason, shard_tag}}
    end
  end

  defp issue_initial_snapshotless_tokens(cluster, epoch, shard_tags) do
    Enum.reduce_while(shard_tags, :ok, fn shard_tag, :ok ->
      case issue_initial_snapshotless_token(cluster, epoch, shard_tag) do
        :ok ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:initial_snapshotless_token_issue_failed, reason, shard_tag}}}
      end
    end)
  end

  defp issue_initial_snapshotless_token(cluster, epoch, shard_tag) do
    case Keyword.get(cluster.node_config(), :object_storage) do
      {module, config} = backend when is_atom(module) and is_list(config) ->
        if Keyword.get(config, :require_pristine_first_boot?, false) do
          token = %{
            kind: :initial_snapshotless_materializer,
            version: 1,
            shard_tag: Keys.shard_tag(shard_tag),
            epoch: epoch,
            state: :issued
          }

          key = initial_snapshotless_token_key(token.shard_tag)

          case ObjectStorage.put_if_not_exists(backend, key, :erlang.term_to_binary(token)) do
            :ok -> :ok
            {:error, :already_exists} -> validate_issued_snapshotless_token(backend, key, token)
            {:error, reason} -> {:error, {:initial_snapshotless_token_write_failed, reason}}
          end
        else
          :ok
        end

      _other ->
        :ok
    end
  end

  defp validate_issued_snapshotless_token(backend, key, expected) do
    case ObjectStorage.get(backend, key) do
      {:ok, data} ->
        case :erlang.binary_to_term(data, [:safe]) do
          ^expected -> :ok
          _other -> {:error, :initial_snapshotless_token_not_reusable}
        end

      {:error, reason} ->
        {:error, {:initial_snapshotless_token_unreadable, reason}}
    end
  rescue
    ArgumentError -> {:error, :invalid_initial_snapshotless_token}
  end

  defp initial_snapshotless_token_key(shard_tag) do
    "foundation/initial_materializer/" <> Base.url_encode64(shard_tag, padding: false)
  end

  defp materializer_params(shard_tag) when is_integer(shard_tag) do
    %{"shard_id" => Keys.shard_tag(shard_tag)}
  end

  # Lock a newly created materializer
  defp lock_new_materializer(service, epoch, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)
    lock_fn.(service, epoch)
  end

  # Start materializer pulling from logs for its shard
  defp start_materializer_pulling(pid, shard_tag, recovery_attempt, context) do
    shard_logs = filter_logs_for_shard(recovery_attempt.logs, shard_tag)

    tsl = %{
      id: TransactionSystemLayout.random_id(),
      epoch: recovery_attempt.epoch,
      director: :unavailable,
      sequencer: recovery_attempt.sequencer,
      rate_keeper: nil,
      proxies: recovery_attempt.proxies,
      resolvers: recovery_attempt.resolvers,
      logs: shard_logs,
      services: recovery_attempt.transaction_services
    }

    # PATCHED (fuu): start replacement materializers from their local durable
    # version if it is behind the planned recovery durable version.
    durable_version =
      materializer_pull_start_version(pid, recovery_attempt.durable_version, context)

    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(pid, durable_version, tsl) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason}}
      {:failure, reason, _ref} -> {:error, {:unlock_failed, reason}}
    end
  end

  defp handle_existing_cluster(recovery_attempt, context) do
    # Read at the newest version determined during log recovery planning
    {_oldest, read_version} = recovery_attempt.version_vector

    # Step 1-2: Find or create materializer for system shard
    with {:ok, {materializer_service_id, materializer_service}} <-
           find_or_create_materializer(recovery_attempt, context),
         # Step 3: Lock for recovery
         {:ok, materializer_pid} <-
           lock_materializer(materializer_service, recovery_attempt.epoch, context),
         recovery_attempt =
           record_existing_materializer_service(
             recovery_attempt,
             materializer_service_id,
             materializer_service,
             materializer_pid
           ),
         # Step 4: Unlock with logs to start pulling
         :ok <- unlock_and_start_pulling(materializer_pid, recovery_attempt, context),
         # Step 5: Wait for catchup
         # PATCHED (fuu): wait persisted system materializer to the planned read version.
         :ok <-
           wait_for_materializer_catchup(
             materializer_pid,
             read_version,
             context
           ),
         # Step 6: Query shard layout
         {:ok, shard_layout} <- get_shard_layout(materializer_pid, read_version, context),
         {:ok, shard_materializers, recovery_attempt} <-
           create_existing_cluster_shard_materializers(
             shard_layout,
             materializer_pid,
             read_version,
             recovery_attempt,
             context
           ) do
      # PATCHED (fuu): derive existing-cluster resolver descriptors from recovered shard layout.
      resolver_descriptors = resolver_descriptors_from_shard_layout(shard_layout)

      updated_attempt =
        recovery_attempt
        |> Map.put(:metadata_materializer, materializer_pid)
        |> Map.put(:shard_layout, shard_layout)
        |> Map.put(:shard_materializers, shard_materializers)
        |> Map.put(:resolvers, resolver_descriptors)

      {updated_attempt, CommitProxyStartupPhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  defp resolver_descriptors_from_shard_layout(shard_layout) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {_tag, start_key} -> start_key end)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.with_index(1)
    |> Enum.map(fn {start_key, index} -> resolver_descriptor(start_key, {:vacancy, index}) end)
  end

  defp create_existing_cluster_shard_materializers(
         shard_layout,
         system_materializer_pid,
         read_version,
         recovery_attempt,
         context
       ) do
    system_shard = RecoveryAttempt.system_shard_id()

    shard_tags =
      shard_layout
      |> extract_shard_tags()
      |> Enum.reject(&(&1 == system_shard))

    if system_materializer_covers_all_shards?(recovery_attempt) and
         not tagged_shard_materializers_present?(shard_tags, context) do
      # PATCHED (fuu): reuse the caught-up system materializer for all shards
      # when the recovered logs are untagged global logs. This avoids launching
      # duplicate cold materializer replays on local single-log clusters.
      # PATCHED (fuu): do not reuse the system materializer when tagged shard
      # materializers exist — they may be empty and still need log catchup.
      Logger.debug(
        "Reusing system materializer for shard tags #{inspect(shard_tags)} because recovered logs are untagged"
      )

      materializers =
        Enum.into(shard_tags, %{system_shard => system_materializer_pid}, fn shard_tag ->
          {shard_tag, system_materializer_pid}
        end)

      {:ok, materializers, recovery_attempt}
    else
      with {:ok, materializers, recovery_attempt} <-
             create_and_catch_up_materializers(
               shard_tags,
               read_version,
               recovery_attempt,
               context
             ) do
        {:ok, Map.put(materializers, system_shard, system_materializer_pid), recovery_attempt}
      end
    end
  end

  defp system_materializer_covers_all_shards?(%{logs: logs}) when is_map(logs) do
    Enum.all?(logs, fn {_log_id, tags} -> tags == [] end)
  end

  defp system_materializer_covers_all_shards?(_recovery_attempt), do: false

  defp tagged_shard_materializers_present?(shard_tags, context) do
    Enum.any?(shard_tags, fn shard_tag ->
      match?({:ok, _service}, find_shard_materializer_service(context, shard_tag))
    end)
  end

  defp create_and_catch_up_materializers(shard_tags, target_version, recovery_attempt, context) do
    Enum.reduce_while(shard_tags, {:ok, %{}, recovery_attempt}, fn shard_tag, {:ok, acc, recovery_attempt} ->
      with {:ok, materializer} <-
             find_or_create_shard_materializer(shard_tag, recovery_attempt, context),
           pid = shard_materializer_pid(materializer),
           :ok <- wait_for_materializer_catchup(pid, target_version, context) do
        # PATCHED (fuu): reuse existing shard materializers during recovery
        # instead of spawning a fresh cold replay on every retry.
        recovery_attempt = record_shard_materializer_service(recovery_attempt, materializer)

        {:cont, {:ok, Map.put(acc, shard_tag, pid), recovery_attempt}}
      else
        {:error, reason} -> {:halt, {:error, {shard_tag, reason}}}
      end
    end)
  end

  defp find_or_create_shard_materializer(shard_tag, recovery_attempt, context) do
    case find_shard_materializer_service(context, shard_tag) do
      {:ok, {service_id, service}} ->
        Logger.debug("Reusing materializer #{inspect(service_id)} for shard #{inspect(shard_tag)}")

        with {:ok, pid} <- lock_materializer(service, recovery_attempt.epoch, context),
             :ok <- start_materializer_pulling(pid, shard_tag, recovery_attempt, context) do
          {:ok, {:existing, service_id, service, pid}}
        end

      {:error, {:materializer_unavailable, :not_in_available_services}} ->
        if Map.get(context, :allow_create_materializer?, false) do
          with {:ok, {service_id, worker_ref, node, pid}} <-
                 create_and_start_materializer(shard_tag, recovery_attempt, context) do
            {:ok, {:created, service_id, worker_ref, node, pid}}
          end
        else
          # PATCHED (fuu): wait for tagged shard materializer advertisement instead of creating a replacement
          Logger.info(
            "Shard #{inspect(shard_tag)} materializer not found among #{inspect(Map.keys(Map.get(context, :available_services, %{})))}; waiting for tagged advertisement"
          )

          {:error, {:materializer_unavailable, :waiting_for_tagged_materializer}}
        end
    end
  end

  defp shard_materializer_pid({:existing, _service_id, _service, pid}), do: pid
  defp shard_materializer_pid({:created, _service_id, _worker_ref, _node, pid}), do: pid

  defp record_shard_materializer_service(recovery_attempt, {:existing, service_id, service, pid}) do
    record_existing_materializer_service(recovery_attempt, service_id, service, pid)
  end

  defp record_shard_materializer_service(recovery_attempt, {:created, service_id, worker_ref, node, pid}) do
    record_materializer_service(recovery_attempt, service_id, worker_ref, node, pid)
  end

  defp record_materializer_service(recovery_attempt, service_id, worker_ref, node, pid) do
    # PATCHED (fuu): record materializer services in recovery topology.
    descriptor = %{kind: :materializer, last_seen: {worker_ref, node}, status: {:up, pid}}

    put_materializer_service(recovery_attempt, service_id, descriptor, pid)
  end

  defp record_existing_materializer_service(recovery_attempt, service_id, service, pid) do
    descriptor = %{
      kind: :materializer,
      last_seen: materializer_last_seen(service),
      status: {:up, pid}
    }

    put_materializer_service(recovery_attempt, service_id, descriptor, pid)
  end

  defp put_materializer_service(recovery_attempt, service_id, descriptor, pid) do
    recovery_attempt
    |> Map.update(:transaction_services, %{service_id => descriptor}, &Map.put(&1, service_id, descriptor))
    |> Map.update(:service_pids, %{service_id => pid}, &Map.put(&1, service_id, pid))
  end

  defp materializer_last_seen({:materializer, last_seen}), do: last_seen
  defp materializer_last_seen({:materializer, last_seen, _shard_id}), do: last_seen
  defp materializer_last_seen({{:materializer, _shard_id}, last_seen}), do: last_seen
  defp materializer_last_seen(%{last_seen: last_seen}), do: last_seen
  defp materializer_last_seen(_service), do: nil

  # Find existing materializer or create a new one for the system shard
  defp find_or_create_materializer(recovery_attempt, context) do
    case find_materializer_service(context) do
      {:ok, {_service_id, _service} = located_service} ->
        {:ok, located_service}

      {:error, {:materializer_unavailable, :not_in_available_services}} ->
        maybe_create_or_stall_system_materializer(recovery_attempt, context)

      {:error, {:materializer_unavailable, :ambiguous_legacy_materializers}} ->
        case most_advanced_legacy_materializer_service(context) do
          {:ok, {_service_id, _service} = located_service, progress} ->
            # PATCHED (fuu): reuse the most advanced legacy materializer instead
            # of spawning a fresh cold replay on every recovery retry.
            Logger.warning(
              "Multiple legacy untagged materializers found; reusing most advanced materializer at #{inspect(progress)}"
            )

            {:ok, located_service}

          {:error, :no_reusable_legacy_materializer} ->
            maybe_create_or_stall_system_materializer(recovery_attempt, context)
        end
    end
  end

  defp maybe_create_or_stall_system_materializer(recovery_attempt, context) do
    if Map.get(context, :allow_create_materializer?, false) do
      # PATCHED (fuu): create a new shard-tagged system materializer
      Logger.info("System shard materializer not found, creating new one")
      create_materializer(recovery_attempt, context)
    else
      # PATCHED (fuu): never create a replacement system materializer during
      # existing-cluster recovery. Creating one consumes the snapshotless token.
      stall_for_tagged_materializer(context, RecoveryAttempt.system_shard_id())
    end
  end

  defp stall_for_tagged_materializer(context, shard_tag) do
    available = Map.keys(Map.get(context, :available_services, %{}))

    Logger.info(
      "System shard materializer not found among #{inspect(available)}; waiting for tagged shard #{inspect(shard_tag)} advertisement"
    )

    {:error, {:materializer_unavailable, :waiting_for_tagged_materializer}}
  end

  # Find materializer assigned to system shard (tag 0)
  # Supports both shard-based lookup (new format) and legacy string-key lookup
  defp find_materializer_service(%{available_services: services} = context) do
    system_shard = RecoveryAttempt.system_shard_id()

    case find_shard_materializer_service(context, system_shard) do
      {:ok, {_id, _service} = located_service} ->
        {:ok, located_service}

      {:error, {:materializer_unavailable, :not_in_available_services}} ->
        # Fall back to legacy string-key lookup for backward compatibility
        case Map.get(services, "metadata_materializer") do
          nil -> legacy_untagged_materializer_service(services)
          service -> {:ok, {"metadata_materializer", service}}
        end
    end
  end

  defp find_shard_materializer_service(%{available_services: services} = context, shard_tag) do
    # PATCHED (fuu): reuse existing shard materializers during recovery.
    # PATCHED (fuu): reuse tagged materializer kinds even when progress cannot be probed.
    matches = Enum.filter(services, &shard_materializer_service?(&1, shard_tag, context))

    case matches do
      [] ->
        {:error, {:materializer_unavailable, :not_in_available_services}}

      candidates ->
        {progress, candidate} = pick_reusable_materializer(candidates, context)
        Logger.debug("Reusing most advanced shard #{inspect(shard_tag)} materializer at #{inspect(progress)}")
        {:ok, candidate}
    end
  end

  defp pick_reusable_materializer(candidates, context) do
    ranked =
      Enum.map(candidates, fn {_id, service} = candidate ->
        {legacy_materializer_progress(service, context), candidate}
      end)

    case Enum.reject(ranked, fn {progress, _candidate} -> is_nil(progress) end) do
      [] ->
        # Directory identity is enough: do not create a replacement materializer
        # that would consume the snapshotless first-boot token.
        {nil, hd(candidates)}

      with_progress ->
        Enum.max_by(with_progress, fn {progress, _candidate} -> progress end)
    end
  end

  defp shard_materializer_service?({_id, {kind, _ref, service_shard_tag}}, shard_tag, _context)
       when is_integer(service_shard_tag) do
    kind == :materializer and service_shard_tag == shard_tag
  end

  defp shard_materializer_service?({_id, {{:materializer, service_shard_tag}, _ref}}, shard_tag, _context)
       when is_integer(service_shard_tag) do
    # PATCHED (fuu): treat {:materializer, shard_id} as a tagged kind, not a worker ref.
    # merge/set_node_resources stores { {{:materializer, shard}, worker_ref} }.
    service_shard_tag == shard_tag
  end

  defp shard_materializer_service?({_id, {:materializer, _ref} = service}, shard_tag, context) do
    # PATCHED (fuu): reuse untagged but runtime shard-assigned materializers.
    runtime_materializer_shard_tag(service, context) == shard_tag
  end

  defp shard_materializer_service?(_service, _shard_tag, _context), do: false

  defp runtime_materializer_shard_tag(service, context) do
    info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)

    case materializer_ref(service) do
      nil ->
        nil

      ref ->
        case info_fn.(ref, [:shard_id]) do
          {:ok, %{shard_id: shard_id}} -> normalize_materializer_shard_tag(shard_id)
          {:ok, %{"shard_id" => shard_id}} -> normalize_materializer_shard_tag(shard_id)
          _ -> nil
        end
    end
  end

  defp normalize_materializer_shard_tag(shard_tag) when is_integer(shard_tag), do: shard_tag

  defp normalize_materializer_shard_tag(shard_tag) when is_binary(shard_tag) do
    case Keys.parse_shard_tag(shard_tag) do
      {:ok, shard_id} -> shard_id
      {:error, _reason} -> nil
    end
  end

  defp normalize_materializer_shard_tag(_shard_tag), do: nil

  defp legacy_untagged_materializer_service(services) do
    # PATCHED (fuu): reuse single legacy system materializer when shard params
    # were not persisted by an older local Bedrock build.
    case Enum.filter(services, &legacy_untagged_materializer_service?/1) do
      [{id, service}] ->
        {:ok, {id, service}}

      [] ->
        {:error, {:materializer_unavailable, :not_in_available_services}}

      _multiple ->
        {:error, {:materializer_unavailable, :ambiguous_legacy_materializers}}
    end
  end

  defp legacy_untagged_materializer_service?({_id, {:materializer, last_seen}}) when not is_integer(last_seen), do: true

  defp legacy_untagged_materializer_service?(_service), do: false

  defp most_advanced_legacy_materializer_service(%{available_services: services} = context) do
    services
    |> Enum.filter(&legacy_untagged_materializer_service?/1)
    |> Enum.reduce([], fn {_id, service} = candidate, acc ->
      case legacy_materializer_progress(service, context) do
        nil -> acc
        progress -> [{progress, candidate} | acc]
      end
    end)
    |> case do
      [] ->
        {:error, :no_reusable_legacy_materializer}

      candidates ->
        {progress, candidate} = Enum.max_by(candidates, fn {progress, _candidate} -> progress end)
        {:ok, candidate, progress}
    end
  end

  defp legacy_materializer_progress(service, context) do
    info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)

    case materializer_ref(service) do
      nil ->
        nil

      ref ->
        case info_fn.(ref, [:current_version, :durable_version]) do
          {:ok, %{current_version: current_version, durable_version: durable_version}}
          when is_binary(current_version) and is_binary(durable_version) ->
            {current_version, durable_version}

          {:ok, %{durable_version: durable_version}} when is_binary(durable_version) ->
            {durable_version, durable_version}

          _ ->
            nil
        end
    end
  end

  defp materializer_ref({:materializer, last_seen}), do: last_seen
  defp materializer_ref({:materializer, last_seen, _shard_id}), do: last_seen
  defp materializer_ref({{:materializer, _shard_id}, last_seen}), do: last_seen
  defp materializer_ref(%{status: {:up, pid}}), do: pid
  defp materializer_ref(%{last_seen: last_seen}), do: last_seen
  defp materializer_ref(_service), do: nil

  # Create a new materializer on a capable node
  defp create_materializer(recovery_attempt, context) do
    with {:ok, node} <- find_materializer_capable_node(context),
         {:ok, {service_id, worker_ref, node}} <- create_materializer_on_node(node, recovery_attempt, context) do
      {:ok, {service_id, {:materializer, {worker_ref, node}, RecoveryAttempt.system_shard_id()}}}
    end
  end

  # Find a node that can host materializers
  defp find_materializer_capable_node(%{node_capabilities: caps}) do
    case Map.get(caps, :materializer, []) do
      [node | _] -> {:ok, node}
      [] -> {:error, :no_materializer_capable_nodes}
    end
  end

  # Create the worker via Foreman with shard_id param
  defp create_materializer_on_node(node, recovery_attempt, context) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}
    worker_id = Worker.random_id()
    system_shard = RecoveryAttempt.system_shard_id()

    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)

    # PATCHED (fuu): pass shard_id params so materializer knows its assignment.
    case create_worker_fn.(foreman_ref, worker_id, :materializer,
           timeout: 30_000,
           params: materializer_params(system_shard)
         ) do
      {:ok, worker_ref} -> {:ok, {worker_id, worker_ref, node}}
      {:error, reason} -> {:error, {:failed_to_create_materializer, reason, system_shard}}
    end
  end

  # Filter logs to only those relevant for the given shard (by tag)
  defp filter_logs_for_shard(logs, shard_id) do
    logs
    |> Enum.filter(fn {_log_id, tags} ->
      # PATCHED (fuu): [] tags = consistent-hash log serves all shards (single-node read fix)
      tags == [] or shard_id in tags
    end)
    |> Map.new()
  end

  # Unlock materializer with only the logs it needs to start pulling
  defp unlock_and_start_pulling(materializer_pid, recovery_attempt, context) do
    # Build TSL with only system shard logs
    system_shard = RecoveryAttempt.system_shard_id()
    system_logs = filter_logs_for_shard(recovery_attempt.logs, system_shard)

    # TransactionSystemLayout is a type, not a struct, so we build a map
    tsl = %{
      id: TransactionSystemLayout.random_id(),
      epoch: recovery_attempt.epoch,
      director: :unavailable,
      sequencer: recovery_attempt.sequencer,
      rate_keeper: nil,
      proxies: recovery_attempt.proxies,
      resolvers: recovery_attempt.resolvers,
      logs: system_logs,
      services: recovery_attempt.transaction_services
    }

    durable_version =
      materializer_pull_start_version(materializer_pid, recovery_attempt.durable_version, context)

    unlock_fn = Map.get(context, :unlock_materializer_fn, &default_unlock_materializer/3)

    case unlock_fn.(materializer_pid, durable_version, tsl) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unlock_failed, reason}}
      {:failure, reason, _ref} -> {:error, {:unlock_failed, reason}}
    end
  end

  defp materializer_pull_start_version(pid, recovery_durable_version, context)
       when is_binary(recovery_durable_version) do
    info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)

    case info_fn.(pid, [:current_version, :durable_version]) do
      {:ok, %{current_version: local_current}} when is_binary(local_current) ->
        if local_current != recovery_durable_version do
          Logger.debug(
            "Materializer local current version #{inspect(local_current)} differs from recovery durable #{inspect(recovery_durable_version)}; pulling from local current version"
          )
        end

        local_current

      {:ok, %{durable_version: local_durable}} when is_binary(local_durable) ->
        if local_durable != recovery_durable_version do
          Logger.debug(
            "Materializer local durable version #{inspect(local_durable)} differs from recovery durable #{inspect(recovery_durable_version)}; pulling from local durable version"
          )
        end

        local_durable

      _ ->
        recovery_durable_version
    end
  end

  defp materializer_pull_start_version(_pid, recovery_durable_version, _context), do: recovery_durable_version

  defp default_unlock_materializer(pid, durable_version, tsl) do
    Materializer.unlock_after_recovery(pid, durable_version, tsl, timeout_in_ms: 30_000)
  end

  # Poll until materializer reaches target version
  defp wait_for_materializer_catchup(pid, target_version, context) do
    timeout_ms = Map.get(context, :catchup_timeout_ms, @catchup_timeout_ms)
    poll_interval_ms = Map.get(context, :catchup_poll_interval_ms, @catchup_poll_interval_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_wait_for_catchup(pid, target_version, deadline, poll_interval_ms, context)
  end

  defp do_wait_for_catchup(pid, target_version, deadline, poll_interval_ms, context) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :catchup_timeout}
    else
      read_ready_fn =
        Map.get(context, :materializer_read_ready_fn, &default_materializer_read_ready?/2)

      case read_ready_fn.(pid, target_version) do
        :ok ->
          # PATCHED (fuu): wait persisted materializers by read readiness, then
          # force a durable checkpoint before publishing recovered topology.
          with :ok <- force_materializer_durable_checkpoint(pid, target_version, context) do
            Logger.debug("Materializer can serve and durably checkpoint read version #{inspect(target_version)}")

            :ok
          end

        {:error, :version_too_new} ->
          log_materializer_wait_status(pid, target_version, context)
          Process.sleep(poll_interval_ms)
          do_wait_for_catchup(pid, target_version, deadline, poll_interval_ms, context)

        {:error, reason} ->
          {:error, {:catchup_readiness_failed, reason}}
      end
    end
  end

  defp force_materializer_durable_checkpoint(pid, target_version, context) do
    checkpoint_fn =
      Map.get(
        context,
        :materializer_force_durable_checkpoint_fn,
        &default_materializer_force_durable_checkpoint/2
      )

    case checkpoint_fn.(pid, target_version) do
      :ok -> :ok
      {:error, reason} -> {:error, {:durable_checkpoint_failed, reason}}
      {:failure, :timeout, _ref} -> {:error, :durable_checkpoint_timeout}
      {:failure, reason, _ref} -> {:error, {:durable_checkpoint_failed, reason}}
    end
  end

  defp default_materializer_force_durable_checkpoint(pid, target_version) do
    Materializer.force_durable_checkpoint(pid, target_version, timeout_in_ms: 30_000)
  end

  defp default_materializer_read_ready?(pid, target_version) do
    # PATCHED (fuu): use current_version for catchup readiness so recovery
    # does not issue full-keyspace range probes while the materializer is still
    # replaying logs. The shard-layout read below remains the real read proof.
    case Materializer.info(pid, [:current_version], timeout_in_ms: 1_000) do
      {:ok, %{current_version: current_version}}
      when is_binary(current_version) and current_version >= target_version ->
        :ok

      {:ok, %{current_version: current_version}} when is_binary(current_version) ->
        {:error, :version_too_new}

      {:ok, %{current_version: {:error, reason}}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}

      {:failure, :timeout, _ref} ->
        {:error, :version_too_new}

      {:failure, reason, _ref} ->
        {:error, reason}
    end
  end

  defp log_materializer_wait_status(pid, target_version, context) do
    info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)

    # PATCHED (fuu): materializer catchup status diagnostics.
    case info_fn.(pid, [:durable_version, :current_version, :intake_queue_size, :mode, :pull_task_alive?]) do
      {:ok, info} ->
        Logger.debug(
          "Materializer cannot yet serve #{inspect(target_version)}; durable_version=#{inspect(info[:durable_version])} current_version=#{inspect(info[:current_version])} intake_queue_size=#{inspect(info[:intake_queue_size])} mode=#{inspect(info[:mode])} pull_task_alive?=#{inspect(info[:pull_task_alive?])}"
        )

      _ ->
        Logger.debug("Materializer cannot yet serve #{inspect(target_version)}")
    end
  end

  defp default_materializer_info(pid, fact_names) do
    Materializer.info(pid, fact_names, timeout_in_ms: 5_000)
  end

  defp lock_materializer(service, epoch, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)
    lock_fn.(service, epoch)
  end

  defp default_lock_materializer({:materializer, name}, epoch) when not is_integer(name) do
    lock_materializer_by_name(name, epoch)
  end

  # Handle new format with shard_id
  defp default_lock_materializer({:materializer, name, _shard_id}, epoch) do
    lock_materializer_by_name(name, epoch)
  end

  defp default_lock_materializer({{:materializer, _shard_id}, name}, epoch) do
    lock_materializer_by_name(name, epoch)
  end

  defp lock_materializer_by_name(name, epoch) do
    name
    |> Materializer.lock_for_recovery(epoch)
    |> case do
      {:ok, pid, _info} -> {:ok, pid}
      {:error, reason} -> {:error, {:materializer_lock_failed, reason}}
    end
  end

  defp get_shard_layout(materializer_pid, read_version, context) do
    get_layout_fn = Map.get(context, :get_shard_layout_fn, &default_get_shard_layout/2)

    case get_layout_fn.(materializer_pid, read_version) do
      {:ok, _layout} = ok ->
        ok

      {:error, {:shard_layout_query_failed, :version_too_old}} ->
        retry_shard_layout_at_materializer_version(materializer_pid, read_version, get_layout_fn, context)

      {:error, :version_too_old} ->
        retry_shard_layout_at_materializer_version(materializer_pid, read_version, get_layout_fn, context)

      other ->
        other
    end
  end

  # PATCHED (fuu): retry shard layout at materializer version when log version is too old.
  # A later sequencer epoch can advance the system materializer past recovered logs, so
  # get_range at the log last version returns :version_too_old even though the layout
  # is readable at the materializer's current version.
  defp retry_shard_layout_at_materializer_version(materializer_pid, read_version, get_layout_fn, context) do
    info_fn = Map.get(context, :materializer_info_fn, &default_materializer_info/2)
    readable_version = readable_materializer_version(info_fn.(materializer_pid, [:current_version, :durable_version]))

    if is_binary(readable_version) and readable_version != read_version do
      Logger.info(
        "PATCHED (fuu): retry shard layout at materializer version when log version is too old; log=#{inspect(read_version)} materializer=#{inspect(readable_version)}"
      )

      case get_layout_fn.(materializer_pid, readable_version) do
        {:ok, _layout} = ok ->
          ok

        {:error, _reason} ->
          Logger.warning(
            "Shard layout unreadable at materializer version #{inspect(readable_version)}; using default two-shard layout"
          )

          {:ok, default_shard_layout()}
      end
    else
      Logger.warning("Shard layout unreadable at log version #{inspect(read_version)}; using default two-shard layout")

      {:ok, default_shard_layout()}
    end
  end

  defp readable_materializer_version({:ok, %{current_version: version}}) when is_binary(version), do: version
  defp readable_materializer_version({:ok, %{durable_version: version}}) when is_binary(version), do: version
  defp readable_materializer_version(_info), do: nil

  defp default_get_shard_layout(materializer_pid, read_version) do
    # Query the materializer for shard layout via get_range on shard_keys prefix
    prefix = Bedrock.SystemKeys.shard_keys_prefix()
    end_key = prefix <> <<0xFF, 0xFF, 0xFF, 0xFF>>

    case Materializer.get_range(materializer_pid, prefix, end_key, read_version, limit: 1000) do
      {:ok, {entries, _more}} ->
        # PATCHED (fuu): shard_key values store tags; reconstruct start_key from sorted end keys.
        shard_layout =
          entries
          |> Enum.map(fn {key, value} ->
            end_key = extract_end_key_from_shard_key(key)
            {end_key, decode_shard_value(value)}
          end)
          |> Enum.sort_by(fn {end_key, _decoded_value} -> end_key end)
          |> Enum.map_reduce(<<>>, fn
            {end_key, {tag, explicit_start_key}}, _previous_end_key ->
              {{end_key, {tag, explicit_start_key}}, end_key}

            {end_key, tag}, previous_end_key ->
              {{end_key, {tag, previous_end_key}}, end_key}
          end)
          |> elem(0)
          |> Map.new()

        if map_size(shard_layout) == 0 do
          # PATCHED (fuu): fallback empty recovered shard layout to default topology.
          Logger.warning(
            "Recovered shard layout is empty; using default two-shard layout for legacy persisted bootstrap"
          )

          {:ok, default_shard_layout()}
        else
          {:ok, shard_layout}
        end

      {:error, reason} ->
        {:error, {:shard_layout_query_failed, reason}}

      {:failure, reason, _ref} ->
        {:error, {:shard_layout_query_failed, reason}}
    end
  end

  defp extract_end_key_from_shard_key(key) do
    prefix = Bedrock.SystemKeys.shard_keys_prefix()
    prefix_len = byte_size(prefix)
    binary_part(key, prefix_len, byte_size(key) - prefix_len)
  end

  defp decode_shard_value(value) when is_binary(value) do
    :erlang.binary_to_term(value)
  end
end
