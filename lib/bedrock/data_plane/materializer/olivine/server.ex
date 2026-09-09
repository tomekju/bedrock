defmodule Bedrock.DataPlane.Materializer.Olivine.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.Reading
  alias Bedrock.DataPlane.Materializer.Olivine.State
  alias Bedrock.DataPlane.Materializer.Telemetry
  alias Bedrock.Service.Foreman

  # Transaction count limits for adaptive batching
  # Small batches for responsiveness during normal operation
  @continuation_batch_count 5
  # Larger batches during lulls when no reads are waiting
  @timeout_batch_count 50

  # Ingest backpressure: above the high-water the ingest reply is withheld
  # (the puller blocks); it is released once the queue drains below the
  # release mark.
  @ingest_high_water_count 1_000
  @ingest_release_count 500

  @spec child_spec(opts :: keyword()) :: map()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    foreman = opts[:foreman] || raise "Missing :foreman option"
    id = opts[:id] || raise "Missing :id option"
    path = opts[:path] || raise "Missing :path option"
    cluster = opts[:cluster]
    params = opts[:params] || %{}
    shard_id = params["shard_id"]

    # Build startup opts only if cluster and shard_id are provided
    startup_opts =
      if cluster && shard_id do
        [cluster: cluster, shard_id: shard_id]
      else
        []
      end

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {otp_name, foreman, id, path, startup_opts},
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init(args), do: {:ok, args, {:continue, :finish_startup}}

  @impl true

  def handle_call({:get, key, version, opts}, from, %State{} = t) do
    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get, key: key})

    fetch_opts = opts |> Keyword.put(:reply_fn, reply_fn_for(from)) |> Keyword.put_new(:wait_ms, 1_000)
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} =
      Reading.handle_get(
        t.read_request_manager,
        context,
        key,
        version,
        fetch_opts
      )

    updated_state = %{t | read_request_manager: updated_manager}
    schedule_waiter_expiration(t.read_request_manager, updated_manager, fetch_opts[:wait_ms])

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
    end
  end

  def handle_call({:get_range, start_key, end_key, version, opts}, from, %State{} = t) do
    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get_range, key: {start_key, end_key}})

    fetch_opts = opts |> Keyword.put(:reply_fn, reply_fn_for(from)) |> Keyword.put_new(:wait_ms, 1_000)
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} =
      Reading.handle_get_range(
        t.read_request_manager,
        context,
        start_key,
        end_key,
        version,
        fetch_opts
      )

    updated_state = %{t | read_request_manager: updated_manager}
    schedule_waiter_expiration(t.read_request_manager, updated_manager, fetch_opts[:wait_ms])

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
    end
  end

  # The puller hands over a batch and waits for :ok. While locked, the
  # puller is being torn down: acknowledge and discard.
  @impl true
  def handle_call({:ingest, _encoded_transactions, _kcv}, _from, %State{mode: :locked} = t), do: reply(t, :ok)

  # Only the current puller may feed the stream. A superseded puller —
  # torn down at compaction cutover or recovery unlock — may have died
  # with an ingest call already in this mailbox; applying that batch
  # would graft a stale suffix, with a gap beneath it, onto the rewound
  # index. Acknowledge and discard. (With no puller at all, direct
  # ingest is the static unit-test configuration and is accepted.)
  def handle_call({:ingest, _encoded_transactions, _kcv}, {caller, _}, %State{pull_task: %Task{pid: pid}} = t)
      when caller !== pid, do: reply(t, :ok)

  def handle_call({:ingest, encoded_transactions, kcv}, from, %State{} = t) do
    updated_intake_queue = IntakeQueue.add_transactions(t.intake_queue, encoded_transactions)
    queue_size = IntakeQueue.size(updated_intake_queue)

    t = %{
      t
      | intake_queue: updated_intake_queue,
        known_committed_version: max_version(t.known_committed_version, kcv)
    }

    Telemetry.trace_transactions_queued(length(encoded_transactions), queue_size)

    if queue_size >= @ingest_high_water_count do
      # Backpressure: hold the reply until the queue drains. The puller
      # cannot outrun the applier because the applier holds the reply.
      noreply(%{t | pending_ingest: from}, continue: :process_transactions)
    else
      reply(t, :ok, continue: :process_transactions)
    end
  end

  @impl true
  def handle_call({:info, fact_names}, _from, %State{} = t), do: t |> Logic.info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    with {:ok, t} <- Logic.lock_for_recovery(t, director, epoch),
         {:ok, info} <- Logic.info(t, Materializer.recovery_info()) do
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  @impl true
  def handle_call({:unlock_after_recovery, durable_version, transaction_system_layout}, {_director, _}, t) do
    {:ok, updated_state} = Logic.unlock_after_recovery(t, durable_version, transaction_system_layout)
    reply(updated_state, :ok)
  end

  @impl true
  def handle_call(:compact, _from, %State{compaction_task: task} = t) when not is_nil(task) do
    # Compaction already in progress
    reply(t, {:error, :compaction_in_progress})
  end

  @impl true
  def handle_call(:compact, _from, %State{} = t) do
    case Logic.start_compaction(t) do
      {:ok, task} ->
        reply(%{t | compaction_task: task, allow_window_advancement: false}, :ok)

      {:error, reason} ->
        reply(t, {:error, reason})
    end
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :not_ready})

  @impl true
  # Handle new 5-tuple format with opts
  def handle_continue(:finish_startup, {otp_name, foreman, id, path, opts}) when is_list(opts) do
    do_finish_startup(otp_name, foreman, id, path, opts)
  end

  # Backward compatibility: handle old 4-tuple format (for tests that bypass child_spec)
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    do_finish_startup(otp_name, foreman, id, path, [])
  end

  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    noreply(t, continue: :process_transactions)
  end

  def handle_continue(:process_transactions, %State{} = t) do
    case IntakeQueue.take_batch_by_count(t.intake_queue, @continuation_batch_count) do
      {[], nil, updated_intake_queue} ->
        # Queue empty, just wait for new transactions or timeout
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        noreply(updated_state)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        # Process small batch for responsiveness
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        final_state = notify_waiting_fetches(state_with_txns, version)

        # Check for more transactions to process
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  def handle_continue(:maybe_process_transactions, %State{} = t) do
    if IntakeQueue.empty?(t.intake_queue) do
      noreply(t, timeout: 0)
    else
      noreply(t, continue: :process_transactions)
    end
  end

  def handle_continue(:advance_window, %State{} = t) do
    if t.allow_window_advancement do
      {:ok, state_after_window} = Logic.advance_window(t)
      noreply(state_after_window)
    else
      # Compaction in progress - skip window advancement
      noreply(t)
    end
  end

  defp do_finish_startup(otp_name, foreman, id, path, opts) do
    # Set persistent telemetry metadata for this server
    Telemetry.trace_metadata(%{otp_name: otp_name, storage_id: id})

    Telemetry.trace_startup_start()

    case Logic.startup(otp_name, foreman, id, path, opts) do
      {:ok, state} ->
        Telemetry.trace_startup_complete()
        noreply(state, continue: :report_health_to_foreman)

      {:error, reason} ->
        Telemetry.trace_startup_failed(reason)
        stop(:no_state, reason)
    end
  end

  defp notify_waiting_fetches(state, version) do
    context = Reading.ReadingContext.new(state.index_manager, state.database)
    updated_manager = Reading.notify_waiting_fetches(state.read_request_manager, context, version)
    %{state | read_request_manager: updated_manager}
  end

  defp maybe_release_ingest(%State{pending_ingest: nil} = t), do: t

  defp maybe_release_ingest(%State{pending_ingest: from} = t) do
    if IntakeQueue.size(t.intake_queue) < @ingest_release_count do
      GenServer.reply(from, :ok)
      %{t | pending_ingest: nil}
    else
      t
    end
  end

  defp max_version(nil, version), do: version
  defp max_version(version, nil), do: version
  defp max_version(a, b), do: max(a, b)

  @impl true
  # Discard transactions when locked
  def handle_info({:apply_transactions, _encoded_transactions}, %State{mode: :locked} = t), do: noreply(t)

  @impl true
  def handle_info({:apply_transactions, encoded_transactions}, %State{} = t) do
    # Queue the transactions and start processing
    updated_intake_queue = IntakeQueue.add_transactions(t.intake_queue, encoded_transactions)
    updated_state = %{t | intake_queue: updated_intake_queue}
    queue_size = IntakeQueue.size(updated_intake_queue)
    Telemetry.trace_transactions_queued(length(encoded_transactions), queue_size)
    Telemetry.trace_transaction_timeout_scheduled()
    noreply(updated_state, continue: :process_transactions)
  end

  @impl true
  def handle_info(:timeout, %State{} = t) do
    # First, process a larger batch of transactions for throughput
    case IntakeQueue.take_batch_by_count(t.intake_queue, @timeout_batch_count) do
      {[], nil, updated_intake_queue} ->
        # No transactions to process, advance window during this lull
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        noreply(updated_state, continue: :advance_window)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        # Process larger batch for throughput
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        state_after_txns = notify_waiting_fetches(state_with_txns, version)

        # Now advance window after processing transactions
        {:ok, final_state} = Logic.advance_window(state_after_txns)
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  @impl true
  def handle_info(:expire_waiting_fetches, %State{} = t) do
    updated_manager = Reading.expire_waiting_fetches(t.read_request_manager)
    noreply(%{t | read_request_manager: updated_manager})
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{} = t) do
    updated_manager = Reading.remove_active_task(t.read_request_manager, pid)
    updated_state = %{t | read_request_manager: updated_manager}
    noreply(updated_state)
  end

  @impl true
  def handle_info(
        {:compaction_ready, compact_data_path, compact_idx_path, new_data_offset, index_offset, compacted_pages,
         durable_version, duration, data_size_before, index_size_before},
        %State{} = t
      ) do
    alias Bedrock.DataPlane.Materializer.Olivine.Telemetry, as: OlivineTelemetry

    case IndexManager.from_compacted_pages(t.index_manager, compacted_pages, durable_version) do
      {:error, reason} ->
        compaction_failed(t, reason)

      {:ok, new_index_manager} ->
        case Database.adopt_compacted_files(
               t.database,
               compact_data_path,
               compact_idx_path,
               new_data_offset,
               index_offset,
               durable_version
             ) do
          {:error, {:swap_rollback_failed, _reason, _undo_errors} = reason} ->
            unrecoverable_compaction_swap(t, reason)

          {:error, reason} ->
            compaction_failed(t, reason)

          {:ok, new_database} ->
            # Rewind after the files are adopted. A failed adopt must not
            # stop the puller or close live descriptors.
            t = Logic.stop_pulling(t)
            {new_data_db, new_index_db} = new_database
            data_path = new_data_db.file_name
            idx_path = new_index_db.file_name

            new_state = %{
              t
              | database: new_database,
                index_manager: new_index_manager,
                intake_queue: IntakeQueue.new(),
                compaction_task: nil,
                allow_window_advancement: true
            }

            values_compacted = Enum.sum(Enum.map(compacted_pages, fn {_, {page, _}} -> Page.key_count(page) end))

            OlivineTelemetry.trace_compaction_complete(durable_version,
              duration_μs: duration,
              data_size_before: data_size_before,
              data_size_after: new_data_offset,
              index_size_before: index_size_before,
              index_size_after: index_offset,
              values_compacted: values_compacted
            )

            Logic.maybe_upload_snapshot(new_state, data_path, idx_path, durable_version)
            noreply(Logic.resume_pulling_from(new_state, durable_version))
        end
    end
  end

  @impl true
  def handle_info({:compaction_failed, reason}, %State{} = t), do: compaction_failed(t, reason)

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(reason, %State{} = t) do
    Telemetry.trace_shutdown_start(reason)
    Reading.shutdown(t.read_request_manager)
    Logic.shutdown(t)
    Telemetry.trace_shutdown_complete()
    :ok
  end

  @impl true
  def terminate(_reason, _state), do: :ok

  defp reply_fn_for(from), do: fn result -> GenServer.reply(from, result) end

  defp schedule_waiter_expiration(previous_manager, updated_manager, wait_ms)
       when is_integer(wait_ms) and wait_ms > 0 do
    if previous_manager.waiting_fetches != updated_manager.waiting_fetches do
      Process.send_after(self(), :expire_waiting_fetches, wait_ms)
    end

    :ok
  end

  defp schedule_waiter_expiration(_previous_manager, _updated_manager, _wait_ms), do: :ok

  defp compaction_failed(%State{} = t, reason) do
    require Logger

    Logger.error("Compaction failed: #{inspect(reason)}")

    {data_db, index_db} = t.database

    try do
      :file.delete(data_db.file_name ++ ~c".compact")
      :file.delete(index_db.file_name ++ ~c".compact")
    catch
      _, _ -> :ok
    end

    noreply(%{t | compaction_task: nil, allow_window_advancement: true})
  end

  defp unrecoverable_compaction_swap(%State{} = t, reason) do
    require Logger

    Logger.error("Compaction swap rollback failed; backups preserved: #{inspect(reason)}")
    stop(t, {:unrecoverable_compaction_swap, reason})
  end
end
