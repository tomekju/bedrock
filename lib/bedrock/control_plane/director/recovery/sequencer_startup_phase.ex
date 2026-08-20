defmodule Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase do
  @moduledoc """
  Solves the fundamental ordering problem by starting the sequencer component that
  provides authoritative global version numbers for all transactions.

  Without global ordering, transaction processing would collapse as different components
  assign conflicting version numbers to concurrent transactions. The sequencer is a
  critical singleton component where only one instance runs cluster-wide to ensure
  consistent version assignment.

  Starts the sequencer process on the director's current node with the last committed
  version from the version vector as the starting point. The sequencer assigns version
  numbers incrementally from this baseline, ensuring no gaps or overlaps in the sequence.
  Configured with current epoch, director PID, and OTP name for service coordination.

  Immediately halts recovery with a fatal error if sequencer startup fails, since version
  assignment is fundamental to transaction processing. Unlike temporary resource shortages,
  sequencer startup failure indicates serious system problems requiring immediate attention.

  Transitions to proxy startup once the sequencer is operational and ready.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Version

  require Logger

  @doc """
  Execute the sequencer startup phase of recovery.

  Starts the sequencer component with the current epoch and last committed version
  from the version vector.
  """
  @impl true
  def execute(recovery_attempt, context) do
    starter_fn = get_starter_function(recovery_attempt, context)
    {_first_version, log_last_version} = recovery_attempt.version_vector
    last_committed_version = Shared.max_committed_version(log_last_version, context)

    case advance_recruited_logs(recovery_attempt, context, log_last_version, last_committed_version) do
      :ok ->
        recovery_attempt
        |> build_sequencer_child_spec(context)
        |> starter_fn.(node())
        |> handle_sequencer_result(recovery_attempt)

      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  # Private helper functions

  @spec get_starter_function(RecoveryAttempt.t(), map()) :: (Supervisor.child_spec(), node() ->
                                                               {:ok, pid()} | {:error, term()})
  defp get_starter_function(recovery_attempt, context) do
    Map.get(context, :start_supervised_fn, fn child_spec, _node ->
      starter_fn = :sup |> recovery_attempt.cluster.otp_name() |> Shared.starter_for()
      starter_fn.(child_spec, node())
    end)
  end

  @spec build_sequencer_child_spec(RecoveryAttempt.t(), map()) :: Supervisor.child_spec()
  defp build_sequencer_child_spec(recovery_attempt, context) do
    {_first_version, log_last_version} = recovery_attempt.version_vector
    last_committed_version = Shared.max_committed_version(log_last_version, context)

    if last_committed_version != log_last_version do
      Logger.info(
        "PATCHED (fuu): start sequencer at max of log and materializer versions; log=#{inspect(log_last_version)} sequencer=#{inspect(last_committed_version)}"
      )
    end

    Sequencer.child_spec(
      cluster: recovery_attempt.cluster,
      director: self(),
      epoch: recovery_attempt.epoch,
      last_committed_version: last_committed_version,
      otp_name: recovery_attempt.cluster.otp_name(:sequencer)
    )
  end

  @spec handle_sequencer_result({:ok, pid()} | {:error, term()}, RecoveryAttempt.t()) ::
          {RecoveryAttempt.t(), module()} | {RecoveryAttempt.t(), {:stalled, term()}}
  defp handle_sequencer_result({:ok, sequencer}, recovery_attempt) do
    updated_recovery_attempt = %{recovery_attempt | sequencer: sequencer}

    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase}
  end

  defp handle_sequencer_result({:error, reason}, recovery_attempt) do
    {recovery_attempt, {:error, {:failed_to_start, :sequencer, node(), reason}}}
  end

  # PATCHED (fuu): when the sequencer will start ahead of recovered logs,
  # jump each recruited log's last_version first. Log.push queues forever
  # when expected_version > last_version, which deadlocks TSL persist.
  defp advance_recruited_logs(_recovery_attempt, _context, log_last_version, last_committed_version)
       when log_last_version == last_committed_version do
    :ok
  end

  defp advance_recruited_logs(recovery_attempt, context, log_last_version, last_committed_version) do
    if version_gt?(last_committed_version, log_last_version) do
      target_version = as_version(last_committed_version)
      log_entries = recruited_log_pids(recovery_attempt)

      if log_entries == [] do
        :ok
      else
        Logger.info(
          "PATCHED (fuu): advance recruited logs to the sequencer start version; log=#{inspect(log_last_version)} target=#{inspect(target_version)}"
        )

        do_advance_recruited_logs(log_entries, target_version, context)
      end
    else
      :ok
    end
  end

  defp do_advance_recruited_logs(log_entries, target_version, context) do
    advance_fn = Map.get(context, :advance_log_fn, &default_advance_log/3)
    injected? = Map.has_key?(context, :advance_log_fn)

    log_entries
    |> Enum.reduce_while(%{}, fn {log_id, pid}, failures ->
      result = advance_one_log(advance_fn, log_id, pid, target_version, injected?)

      case result do
        :ok -> {:cont, failures}
        {:error, reason} -> {:cont, Map.put(failures, log_id, reason)}
        other -> {:cont, Map.put(failures, log_id, other)}
      end
    end)
    |> case do
      failures when failures == %{} -> :ok
      failures -> {:error, {:failed_to_advance_logs, failures}}
    end
  end

  defp recruited_log_pids(recovery_attempt) do
    logs = Map.get(recovery_attempt, :logs) || %{}
    service_pids = Map.get(recovery_attempt, :service_pids) || %{}

    Enum.map(Map.keys(logs), fn log_id -> {log_id, Map.get(service_pids, log_id)} end)
  end

  defp advance_one_log(_advance_fn, log_id, nil, _target_version, _injected?) do
    {:error, {:missing_log_pid, log_id}}
  end

  defp advance_one_log(advance_fn, log_id, pid, target_version, true) do
    advance_fn.(log_id, pid, target_version)
  end

  defp advance_one_log(advance_fn, log_id, pid, target_version, false) do
    parent = self()
    request_ref = make_ref()

    {spawned_pid, monitor_ref} =
      spawn_monitor(fn ->
        send(parent, {:advance_log_result, request_ref, advance_fn.(log_id, pid, target_version)})
      end)

    receive do
      {:advance_log_result, ^request_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^spawned_pid, reason} ->
        {:error, reason}
    after
      30_000 ->
        Process.exit(spawned_pid, :kill)
        {:error, :timeout}
    end
  end

  defp default_advance_log(_log_id, pid, target_version) do
    Log.advance_last_version(pid, target_version)
  end

  defp as_version(version) when is_integer(version) and version >= 0, do: Version.from_integer(version)
  defp as_version(<<_::unsigned-big-64>> = version), do: version

  defp version_gt?(left, right) do
    version_rank(left) > version_rank(right)
  end

  defp version_rank(version) when is_integer(version), do: version
  defp version_rank(<<version::unsigned-big-64>>), do: version
end
