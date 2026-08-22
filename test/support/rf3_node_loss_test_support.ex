defmodule Bedrock.Test.RF3NodeLossCluster do
  @moduledoc false

  use Bedrock.Cluster,
    otp_app: :bedrock,
    name: "rf3_node_loss_rejoin"
end

defmodule Bedrock.Test.RF3NodeLossRepo do
  @moduledoc false

  use Bedrock.Repo, cluster: Bedrock.Test.RF3NodeLossCluster
end

defmodule Bedrock.Test.RF3NodeLossSupport do
  @moduledoc false

  alias Bedrock.Cluster.Link
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.ObjectStorage
  alias Bedrock.Test.AdmittedLocalFilesystem
  alias Bedrock.Test.RF3NodeLossCluster, as: Cluster
  alias Bedrock.Test.RF3NodeLossRepo, as: Repo

  @supervisor __MODULE__.Supervisor
  @recovery_handler {__MODULE__, :recovery_events}
  @recovery_tracker __MODULE__.RecoveryTracker
  @recovery_events [
    [:bedrock, :recovery, :started],
    [:bedrock, :recovery, :suitable_logs_chosen],
    [:bedrock, :recovery, :old_logs_replayed],
    [:bedrock, :recovery, :system_state_persisted],
    [:bedrock, :recovery, :completed],
    [:bedrock, :recovery, :stalled],
    [:bedrock, :recovery, :failed]
  ]

  def cluster_name, do: Cluster.name()

  def start(owner, node_root, descriptor_path, object_storage_root)
      when is_pid(owner) and is_binary(node_root) and is_binary(descriptor_path) and is_binary(object_storage_root) do
    {:ok, _started_applications} = Application.ensure_all_started(:bedrock)
    {:ok, first_boot_admission_tracker} = Agent.start(fn -> 0 end)

    {:ok, _telemetry_tracker} =
      Agent.start(fn -> %{owner: owner, recovery: %{}} end, name: @recovery_tracker)

    object_storage =
      ObjectStorage.backend(AdmittedLocalFilesystem,
        root: object_storage_root,
        first_boot_admission_tracker: first_boot_admission_tracker
      )

    Application.put_env(
      :bedrock,
      Cluster,
      cluster_config(node_root, descriptor_path, object_storage)
    )

    :ok = attach_recovery_handler(owner)

    with {:ok, supervisor} <-
           Supervisor.start_link(
             [{Cluster, []}],
             strategy: :one_for_one,
             name: @supervisor
           ) do
      Process.unlink(supervisor)
      {:ok, supervisor}
    end
  end

  def stop do
    result =
      case Process.whereis(@supervisor) do
        nil -> :ok
        supervisor -> Supervisor.stop(supervisor, :normal, 10_000)
      end

    :telemetry.detach(@recovery_handler)
    stop_telemetry_tracker()
    Application.delete_env(:bedrock, Cluster)
    result
  end

  def write(key, value) when is_binary(key) and is_binary(value) do
    Repo.transact(
      fn ->
        :ok = Repo.put(key, value)
        :written
      end,
      timeout_in_ms: 30_000,
      retry_limit: 30
    )
  end

  def read(key) when is_binary(key) do
    Repo.transact(
      fn -> Repo.get(key) end,
      timeout_in_ms: 30_000,
      retry_limit: 30
    )
  end

  def fetch_config, do: Cluster.fetch_config()

  def fetch_coordinator_node do
    with {:ok, coordinator} <-
           Link.fetch_coordinator(Cluster.link!(), timeout_in_ms: 5_000),
         true <- is_pid(coordinator) do
      {:ok, node(coordinator)}
    else
      false -> {:error, :invalid_coordinator}
      {:error, _reason} = error -> error
    end
  end

  def fetch_layouts do
    with {:ok, authoritative} <- Cluster.fetch_transaction_system_layout(),
         {:ok, link} <-
           Link.fetch_transaction_system_layout(Cluster.link!(), timeout_in_ms: 5_000) do
      {:ok, %{authoritative: authoritative, link: link}}
    end
  end

  def attempt_write(key, value) when is_binary(key) and is_binary(value) do
    {:ok,
     Repo.transact(
       fn ->
         :ok = Repo.put(key, value)
         :written
       end,
       timeout_in_ms: 500,
       retry_limit: 0
     )}
  rescue
    error in RuntimeError -> {:error, Exception.message(error)}
  end

  @doc false
  def fetch_user_shard_materializer_facts do
    with {:ok, %{authoritative: layout}} <- fetch_layouts() do
      layout.shard_materializers
      |> Enum.reject(fn {shard_id, _materializer} -> shard_id == 0 end)
      |> Enum.reduce_while({:ok, %{}}, fn {shard_id, materializer}, {:ok, facts_by_shard} ->
        case Materializer.info(
               materializer,
               [:kind, :shard_id, :current_version, :durable_version],
               timeout_in_ms: 5_000
             ) do
          {:ok, facts} ->
            {:cont, {:ok, Map.put(facts_by_shard, shard_id, Map.put(facts, :materializer, materializer))}}

          {:error, reason} ->
            {:halt, {:error, {:materializer_unavailable, shard_id, reason}}}
        end
      end)
    end
  end

  @doc false
  def handle_recovery_event(event, _measurements, metadata, tracker) do
    %{owner: owner, recovery: recovery} =
      Agent.get_and_update(tracker, fn %{recovery: recovery} = state ->
        updated_recovery =
          case event do
            [:bedrock, :recovery, :started] ->
              %{epoch: Map.fetch!(metadata, :epoch), attempt: Map.fetch!(metadata, :attempt)}

            _ ->
              recovery
          end

        {%{owner: state.owner, recovery: updated_recovery}, %{state | recovery: updated_recovery}}
      end)

    send(owner, {
      :rf3_recovery,
      %{
        attempt: Map.get(recovery, :attempt),
        epoch: Map.get(recovery, :epoch),
        event: List.last(event),
        metadata: metadata,
        node: Node.self(),
        recovery_process: self()
      }
    })
  end

  defp attach_recovery_handler(_owner) do
    case :telemetry.attach_many(
           @recovery_handler,
           @recovery_events,
           &__MODULE__.handle_recovery_event/4,
           @recovery_tracker
         ) do
      :ok ->
        :ok

      {:error, :already_exists} ->
        :telemetry.detach(@recovery_handler)

        :telemetry.attach_many(
          @recovery_handler,
          @recovery_events,
          &__MODULE__.handle_recovery_event/4,
          @recovery_tracker
        )
    end
  end

  defp stop_telemetry_tracker do
    case Process.whereis(@recovery_tracker) do
      nil -> :ok
      tracker -> Agent.stop(tracker, :normal, 5_000)
    end
  end

  defp cluster_config(node_root, descriptor_path, object_storage) do
    working_path = Path.join(node_root, "working")

    [
      capabilities: [:coordination, :log, :materializer],
      path_to_descriptor: descriptor_path,
      object_storage: object_storage,
      desired_logs: 3,
      desired_replication_factor: 3,
      parameters: %{
        desired_coordinators: 3,
        desired_logs: 3,
        desired_replication_factor: 3
      },
      coordinator: [path: Path.join(working_path, "coordinator")],
      log: [path: working_path, object_storage: object_storage],
      materializer: [path: working_path, object_storage: object_storage],
      durability_mode: :strict
    ]
  end
end
