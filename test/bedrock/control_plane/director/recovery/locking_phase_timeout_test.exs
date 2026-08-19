defmodule Bedrock.ControlPlane.Director.Recovery.LockingPhaseTimeoutTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.LockingPhase
  alias Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase

  describe "lock_old_system_services/3" do
    test "ignores Task.async_stream lock timeouts instead of crashing" do
      services = %{
        "uqnjmhjl" => {:log, {:bedrock_fuu_worker_uqnjmhjl, node()}}
      }

      context = %{
        lock_services_timeout_ms: 30,
        lock_service_fn: fn _service, _epoch ->
          Process.sleep(200)
          {:ok, self(), %{kind: :log}}
        end
      }

      assert {:error, :waiting_for_log_locks} =
               LockingPhase.lock_old_system_services(services, 2, context)
    end

    test "locks services that reply before the timeout" do
      pid = self()

      services = %{
        "uqnjmhjl" => {:log, {:bedrock_fuu_worker_uqnjmhjl, node()}}
      }

      context = %{
        lock_service_fn: fn _service, _epoch ->
          {:ok, pid, %{kind: :log}}
        end
      }

      assert {:ok, locked_ids, log_info, _storage_info, _services, pids} =
               LockingPhase.lock_old_system_services(services, 2, context)

      assert MapSet.member?(locked_ids, "uqnjmhjl")
      assert Map.has_key?(log_info, "uqnjmhjl")
      assert pids["uqnjmhjl"] == pid
    end
  end

  describe "execute/2" do
    test "stalls existing-cluster recovery when old logs cannot be locked in time" do
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:epoch, 5)
        |> Map.put(:logs, %{"uqnjmhjl" => []})

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"uqnjmhjl" => []}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "uqnjmhjl" => {:log, {:bedrock_fuu_worker_uqnjmhjl, node()}}
        })
        |> Map.put(:lock_services_timeout_ms, 30)
        |> Map.put(:lock_service_fn, fn _service, _epoch ->
          Process.sleep(200)
          {:ok, self(), %{kind: :log}}
        end)

      assert {_attempt, {:stalled, :waiting_for_log_locks}} =
               LockingPhase.execute(recovery_attempt, context)
    end

    test "continues to log recovery planning when a log locks" do
      pid = self()

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:epoch, 5)
        |> Map.put(:logs, %{"uqnjmhjl" => []})

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"uqnjmhjl" => []}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "uqnjmhjl" => {:log, {:bedrock_fuu_worker_uqnjmhjl, node()}}
        })
        |> Map.put(:lock_service_fn, fn _service, _epoch ->
          {:ok, pid, %{kind: :log}}
        end)

      assert {updated_attempt, LogRecoveryPlanningPhase} =
               LockingPhase.execute(recovery_attempt, context)

      assert MapSet.member?(updated_attempt.locked_service_ids, "uqnjmhjl")
    end
  end
end
