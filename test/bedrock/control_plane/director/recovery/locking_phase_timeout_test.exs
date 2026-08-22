defmodule Bedrock.ControlPlane.Director.Recovery.LockingPhaseTimeoutTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.LockingPhase
  alias Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase

  setup do
    {:ok, recovery_task_supervisor: start_supervised!({Task.Supervisor, []})}
  end

  test "normalizes a timed-out old log lock without serializing recovery", %{
    recovery_task_supervisor: recovery_task_supervisor
  } do
    old_system_services = %{"old-log" => {:log, {:old_log, node()}}}

    context = %{
      recovery_task_supervisor: recovery_task_supervisor,
      lock_services_timeout_ms: 10,
      lock_service_fn: fn _service, _epoch ->
        receive do
          :never -> :ok
        end
      end
    }

    assert {:ok, locked_ids, %{}, %{}, %{}, %{}, timed_out_log_ids} =
             LockingPhase.lock_old_system_services(old_system_services, 1, context)

    assert MapSet.equal?(locked_ids, MapSet.new())
    assert MapSet.equal?(timed_out_log_ids, MapSet.new(["old-log"]))
  end

  test "normalizes a crashing lock task without classifying it as a timeout", %{
    recovery_task_supervisor: recovery_task_supervisor
  } do
    old_system_services = %{"old-log" => {:log, {:old_log, node()}}}

    context = %{
      recovery_task_supervisor: recovery_task_supervisor,
      lock_service_fn: fn _service, _epoch ->
        exit(:lock_crashed)
      end
    }

    assert {:ok, locked_ids, %{}, %{}, %{}, %{}, timed_out_log_ids} =
             LockingPhase.lock_old_system_services(old_system_services, 1, context)

    assert MapSet.equal?(locked_ids, MapSet.new())
    assert MapSet.equal?(timed_out_log_ids, MapSet.new())
  end

  test "records an immediate log lock timeout on the recovery attempt", %{
    recovery_task_supervisor: recovery_task_supervisor
  } do
    recovery_attempt = RecoveryAttempt.new(__MODULE__, 1, DateTime.utc_now())

    context = %{
      recovery_task_supervisor: recovery_task_supervisor,
      old_transaction_system_layout: %{logs: %{"old-log" => []}},
      available_services: %{"old-log" => {:log, {:old_log, node()}}},
      lock_service_fn: fn _service, _epoch -> {:error, :timeout} end
    }

    assert {%{transient_log_lock_timeout_ids: timed_out_log_ids}, LogRecoveryPlanningPhase} =
             LockingPhase.execute(recovery_attempt, context)

    assert MapSet.equal?(timed_out_log_ids, MapSet.new(["old-log"]))
  end
end
