defmodule Bedrock.ControlPlane.Director.Recovery.LogReplayPhase do
  @moduledoc """
  Migrates the active transaction window from old logs to newly recruited log configuration.

  Solves the critical data migration challenge of transferring the current window of committed
  transactions to known-good storage. Since recovery must read old logs anyway to verify what's
  recoverable, it duplicates that data to new logs rather than trusting potentially corrupted
  or failing storage. This ensures storage servers can continue processing from reliable storage.

  **Migration Strategy (Consistent Hashing)**: With consistent hashing, shard→log mapping is
  computed at runtime. Each new log receives the full list of survivors (old logs that were
  locked) and can pull transactions from any of them. The log's personalized transaction
  stream is reconstructed by pulling from survivors and filtering by shard index.

  For initial recovery (no old logs), each new log is initialized with an empty transaction
  stream at version zero.

  **Service Access**: All log services (old and new) are locked by this point in recovery,
  so the phase simply retrieves their PIDs from the `service_pids` map using `Map.fetch!`
  to enforce that all required services must be available.

  **Data Integrity**: Only copies committed transactions within the established version
  vector (the active window), discarding uncommitted transactions that lack durability
  guarantees. Maintains transaction ordering and ensures storage servers can continue
  processing seamlessly in the new configuration.

  Stalls with detailed failure information if log copying fails due to service issues.
  However, immediately halts with error if any log reports `:newer_epoch_exists` (this
  director has been superseded). Transitions to sequencer startup with complete migration.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.DataPlane.Log

  @impl true
  def execute(recovery_attempt, context) do
    # PATCHED (fuu): replacement materializers may start below the planned
    # recovery durable version when no snapshot exists, so replay from the lower
    # of the original version vector and durable version.
    {original_first, last_version} = recovery_attempt.version_vector
    replay_first_version = min_version(original_first, recovery_attempt.durable_version)
    optimized_version_vector = {replay_first_version, last_version}

    # Get survivor log IDs - all logs that were successfully locked during recovery
    survivor_log_ids = Map.get(recovery_attempt, :survivor_log_ids, recovery_attempt.old_log_ids_to_copy)

    survivor_log_ids
    |> replay_into_new_logs(
      Map.keys(recovery_attempt.logs),
      optimized_version_vector,
      recovery_attempt,
      context
    )
    |> case do
      :ok ->
        trace_recovery_old_logs_replayed()
        {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase}

      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  @doc """
  Replays transactions from survivor logs into new logs.

  With consistent hashing, each new log receives the full list of survivors and can
  pull from any of them. The Shale recovery logic handles trying multiple sources
  if the first is unavailable.

  For initial recovery (no survivors), new logs are initialized at the version vector.
  """
  @spec replay_into_new_logs(
          survivor_log_ids :: [Log.id()],
          new_log_ids :: [Log.id()],
          version_vector :: Bedrock.version_vector(),
          recovery_attempt :: map(),
          context :: map()
        ) ::
          :ok
          | {:error, {:failed_to_copy_some_logs, %{Log.id() => term()}}}
  def replay_into_new_logs(
        survivor_log_ids,
        new_log_ids,
        {first_version, last_version},
        recovery_attempt,
        context \\ %{}
      ) do
    copy_log_data_fn = Map.get(context, :copy_log_data_fn, &copy_log_data/5)
    service_pids = recovery_attempt.service_pids

    # Build list of survivor PIDs from their IDs
    survivor_pids = Enum.map(survivor_log_ids, &Map.get(service_pids, &1))
    survivor_pids = Enum.reject(survivor_pids, &is_nil/1)

    # PATCHED (fuu): copy logs sequentially in the director process.
    # Task.async_stream from a GenServer recovery callback drops Task alias
    # replies on Elixir 1.20 and stalls recovery after log locks succeed.
    new_log_ids
    |> Enum.reduce_while(%{}, fn new_log_id, failures ->
      case copy_log_data_fn.(new_log_id, survivor_pids, first_version, last_version, service_pids) do
        {:error, :newer_epoch_exists} = error ->
          {:halt, error}

        {:ok, _pid} ->
          {:cont, failures}

        {:error, reason} ->
          {:cont, Map.put(failures, new_log_id, reason)}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      failures when failures == %{} -> :ok
      failures -> {:error, {:failed_to_copy_some_logs, failures}}
    end
  end

  # Backward compatibility: delegate to new function
  @spec replay_old_logs_into_new_logs(
          old_log_ids :: [Log.id()],
          new_log_ids :: [Log.id()],
          version_vector :: Bedrock.version_vector(),
          recovery_attempt :: map(),
          context :: map()
        ) ::
          :ok
          | {:error, {:failed_to_copy_some_logs, %{Log.id() => term()}}}
  def replay_old_logs_into_new_logs(old_log_ids, new_log_ids, version_vector, recovery_attempt, context \\ %{}) do
    replay_into_new_logs(old_log_ids, new_log_ids, version_vector, recovery_attempt, context)
  end

  defp min_version(left, right) when is_binary(left) and is_binary(right) and left <= right, do: left
  defp min_version(left, right) when is_binary(left) and is_binary(right), do: right

  @doc """
  Copies transaction data from survivor logs to a new log.

  The new log receives a list of survivor PIDs and can pull from any of them.
  For initial recovery (empty survivor list), the log is initialized at the version vector.
  """
  @spec copy_log_data(
          new_log_id :: Log.id(),
          survivor_pids :: [pid()],
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version(),
          service_pids :: %{Log.id() => pid()}
        ) :: {:ok, pid()} | {:error, term()}
  def copy_log_data(new_log_id, survivor_pids, first_version, last_version, service_pids) do
    Log.recover_from(
      Map.fetch!(service_pids, new_log_id),
      survivor_pids,
      first_version,
      last_version
    )
  end

  # Legacy pairing function kept for backward compatibility with tests
  @spec pair_with_old_log_ids([Log.id()], [Log.id()]) ::
          Enumerable.t({Log.id(), Log.id() | :none})
  def pair_with_old_log_ids(new_log_ids, []), do: Stream.zip(new_log_ids, Stream.cycle([:none]))

  def pair_with_old_log_ids(new_log_ids, old_log_ids), do: Stream.zip(new_log_ids, Stream.cycle(old_log_ids))
end
