defmodule Bedrock.ControlPlane.Director.Recovery.LockingPhase do
  @moduledoc """
  Establishes exclusive director control by selectively locking services from the old system layout.

  Service locking serves three purposes: prevent split-brain scenarios where multiple directors
  attempt concurrent control, halt all transaction processing from the old system, and validate
  service reachability while collecting recovery state information (transaction versions,
  durability status).

  Only services referenced in the old transaction system layout are locked - these contain
  data that must be preserved during recovery. Individual service failures (unreachable, timeout)
  are ignored since recovery gathers as many services as possible from a potentially failed system.
  However, if any service is already locked with a newer epoch, this director has been superseded
  and should stop all recovery attempts.

  The recovery path is determined by whether the old layout contained logs: no logs means
  first-time initialization, logs present means recovery from existing data.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Service.Worker

  require Logger

  @impl true
  def execute(recovery_attempt, context) do
    old_system_services =
      extract_old_system_services(
        context.old_transaction_system_layout,
        context.available_services
      )

    old_system_services
    |> lock_old_system_services(recovery_attempt.epoch, context)
    |> case do
      {:error, :newer_epoch_exists} = error ->
        {recovery_attempt, error}

      {:error, :waiting_for_log_locks} ->
        # PATCHED (fuu): stall when old logs exist but none locked in time.
        Logger.info("Old log services did not lock in time; waiting to retry")
        {recovery_attempt, {:stalled, :waiting_for_log_locks}}

      {:ok, locked_service_ids, log_recovery_info_by_id, materializer_recovery_info_by_id, transaction_services,
       service_pids} ->
        updated_recovery_attempt =
          recovery_attempt
          |> Map.update!(:log_recovery_info_by_id, &Map.merge(log_recovery_info_by_id, &1))
          |> Map.update!(
            :materializer_recovery_info_by_id,
            &Map.merge(materializer_recovery_info_by_id, &1)
          )
          |> Map.put(:locked_service_ids, locked_service_ids)
          |> Map.update!(:transaction_services, &Map.merge(transaction_services, &1))
          |> Map.update!(:service_pids, &Map.merge(service_pids, &1))

        {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase}
    end
  end

  @spec lock_old_system_services_timeout() :: Bedrock.timeout_in_ms()
  def lock_old_system_services_timeout, do: 10_000

  @spec lock_old_system_services(
          %{Worker.id() => %{kind: atom(), last_seen: {atom(), node()}}},
          Bedrock.epoch(),
          map()
        ) ::
          {:ok, locked_ids :: MapSet.t(Worker.id()), new_log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()},
           new_materializer_recovery_info_by_id :: %{Materializer.id() => Materializer.recovery_info()},
           transaction_services :: %{
             Worker.id() => %{
               status: {:up, pid()},
               kind: :log | :materializer,
               last_seen: {atom(), node()}
             }
           }, service_pids :: %{Worker.id() => pid()}}
          | {:error, :newer_epoch_exists}
          | {:error, :waiting_for_log_locks}
  def lock_old_system_services(old_system_services, epoch, context \\ %{}) do
    timeout_in_ms = Map.get(context, :lock_services_timeout_ms, lock_old_system_services_timeout())

    # PATCHED (fuu): lock old logs sequentially in the director process.
    # Task.async_stream inside a GenServer recovery callback can time out even
    # when the same lock_for_recovery calls succeed from rpc in milliseconds.
    old_system_services
    |> Enum.reduce_while({MapSet.new(), %{}, %{}, %{}}, fn {id, service}, acc ->
      case lock_one_old_system_service(service, epoch, context, timeout_in_ms) do
        {:error, :newer_epoch_exists} = error ->
          {:halt, error}

        {:ok, pid, info} ->
          {locked_ids, info_by_id, transaction_services, service_pids} = acc

          {:cont,
           {MapSet.put(locked_ids, id), Map.put(info_by_id, id, info),
            Map.put(transaction_services, id, %{
              status: {:up, pid},
              kind: info.kind,
              last_seen:
                case service do
                  {_kind, location} -> location
                  %{last_seen: location} -> location
                end
            }), Map.put(service_pids, id, pid)}}

        {:error, _reason} ->
          {:cont, acc}
      end
    end)
    |> case do
      {:error, _reason} = error ->
        error

      {locked_ids, info_by_id, transaction_services, service_pids} ->
        if map_size(old_system_services) > 0 and MapSet.size(locked_ids) == 0 do
          {:error, :waiting_for_log_locks}
        else
          grouped_recovery_info = Enum.group_by(info_by_id, &Map.get(elem(&1, 1), :kind))
          new_log_recovery_info_by_id = grouped_recovery_info |> Map.get(:log, []) |> Map.new()

          new_materializer_recovery_info_by_id =
            grouped_recovery_info |> Map.get(:materializer, []) |> Map.new()

          {:ok, locked_ids, new_log_recovery_info_by_id, new_materializer_recovery_info_by_id, transaction_services,
           service_pids}
        end
    end
  end

  @spec lock_service_for_recovery(
          {atom(), {atom(), node()}},
          Bedrock.epoch(),
          map()
        ) ::
          {:ok, pid(), map()} | {:error, term()}
  def lock_service_for_recovery(service, epoch, context \\ %{}) do
    lock_one_old_system_service(
      service,
      epoch,
      context,
      Map.get(context, :lock_services_timeout_ms, lock_old_system_services_timeout())
    )
  end

  @spec lock_one_old_system_service(
          {atom(), {atom(), node()}},
          Bedrock.epoch(),
          map(),
          Bedrock.timeout_in_ms()
        ) :: {:ok, pid(), map()} | {:error, term()}
  defp lock_one_old_system_service(service, epoch, context, timeout_in_ms) do
    default_lock = fn svc, ep -> lock_service_impl(svc, ep, timeout_in_ms) end
    lock_fn = Map.get(context, :lock_service_fn, default_lock)

    # PATCHED (fuu): never GenServer.call a log from the director process.
    # lock_for_recovery succeeds from rpc in milliseconds, but the same call
    # from inside a Director callback blocks and leaves locked_service_ids empty.
    if Map.has_key?(context, :lock_service_fn) do
      lock_fn.(service, epoch)
    else
      lock_in_isolated_process(lock_fn, service, epoch, timeout_in_ms)
    end
  end

  defp lock_in_isolated_process(lock_fn, service, epoch, timeout_in_ms) do
    parent = self()
    request_ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        send(parent, {:lock_result, request_ref, lock_fn.(service, epoch)})
      end)

    receive do
      {:lock_result, ^request_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        {:error, reason}
    after
      timeout_in_ms ->
        Process.exit(pid, :kill)
        {:error, :timeout}
    end
  end

  @spec lock_service_impl({atom(), {atom(), node()}}, Bedrock.epoch(), Bedrock.timeout_in_ms()) ::
          {:ok, pid(), map()} | {:error, term()}
  defp lock_service_impl({:log, name}, epoch, timeout_in_ms),
    do: Worker.lock_for_recovery(name, epoch, timeout_in_ms: timeout_in_ms)

  defp lock_service_impl({:materializer, name}, epoch, timeout_in_ms),
    do: Worker.lock_for_recovery(name, epoch, timeout_in_ms: timeout_in_ms)

  defp lock_service_impl(_, _, _), do: {:error, :unavailable}

  @spec extract_old_system_services(map(), %{
          Worker.id() => {atom(), {atom(), node()}}
        }) ::
          %{Worker.id() => {atom(), {atom(), node()}}}
  defp extract_old_system_services(old_layout, available_services) do
    # Only extract log service IDs - storage teams are retired
    old_log_ids =
      old_layout
      |> Map.get(:logs, %{})
      |> Map.keys()
      |> MapSet.new()

    available_services
    |> Enum.filter(fn {service_id, _} ->
      MapSet.member?(old_log_ids, service_id)
    end)
    |> Map.new()
  end
end
