defmodule Bedrock.ControlPlane.Director.RecoveryRetryTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery
  alias Bedrock.ControlPlane.Director.State

  defp recovery_state(overrides \\ %{}) do
    state = %State{
      state: :recovery,
      epoch: 7,
      recovery_attempt: %RecoveryAttempt{attempt: 4}
    }

    Map.merge(state, overrides)
  end

  test "schedules only the tagged transient lock-timeout stall" do
    state = recovery_state()

    assert ^state = Recovery.schedule_transient_recovery_retry(state, :unable_to_meet_log_quorum)

    scheduled =
      Recovery.schedule_transient_recovery_retry(state, {:transient_log_lock_timeouts, ["old-log"]})

    assert %{epoch: 7, stalled_attempt: 4, retry_no: 1, token: token, timer_ref: timer_ref} =
             scheduled.recovery_retry

    assert is_reference(token)
    assert is_reference(timer_ref)

    assert scheduled.recovery_retry ==
             Recovery.schedule_transient_recovery_retry(
               scheduled,
               {:transient_log_lock_timeouts, ["old-log"]}
             ).recovery_retry

    assert %{recovery_retry: nil} = Recovery.reset_transient_recovery_retry(scheduled)
  end

  test "caps transient retries and rejects stale retry messages" do
    exhausted =
      recovery_state(%{
        recovery_retry: %{
          epoch: 7,
          stalled_attempt: 4,
          retry_no: 3,
          token: make_ref(),
          timer_ref: nil
        }
      })

    assert ^exhausted =
             Recovery.schedule_transient_recovery_retry(
               exhausted,
               {:transient_log_lock_timeouts, ["old-log"]}
             )

    scheduled =
      Recovery.schedule_transient_recovery_retry(
        recovery_state(),
        {:transient_log_lock_timeouts, ["old-log"]}
      )

    %{token: token} = scheduled.recovery_retry

    assert :stale = Recovery.claim_transient_recovery_retry(scheduled, 8, 4, token)
    assert :stale = Recovery.claim_transient_recovery_retry(scheduled, 7, 5, token)
    assert :stale = Recovery.claim_transient_recovery_retry(scheduled, 7, 4, make_ref())

    assert {:ok, %{recovery_retry: %{timer_ref: nil, retry_no: 1}}} =
             Recovery.claim_transient_recovery_retry(scheduled, 7, 4, token)

    assert %{recovery_retry: nil} = Recovery.reset_transient_recovery_retry(scheduled)
  end
end
