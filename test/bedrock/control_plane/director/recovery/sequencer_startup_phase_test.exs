defmodule Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase
  alias Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase
  alias Bedrock.DataPlane.Sequencer.Server

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:sup), do: :test_supervisor
    def otp_name(:sequencer), do: :test_sequencer
  end

  # Helper functions for common setup patterns
  defp create_recovery_attempt(cluster, epoch, version_vector, sequencer \\ nil) do
    recovery_attempt()
    |> with_cluster(cluster)
    |> with_epoch(epoch)
    |> with_version_vector(version_vector)
    |> with_sequencer(sequencer)
  end

  defp create_capture_agent do
    {:ok, agent} = Agent.start_link(fn -> nil end)
    agent
  end

  describe "execute/1" do
    test "transitions to error state when sequencer creation fails" do
      # This test verifies that the phase properly handles failures
      # We can't easily mock the DynamicSupervisor call, but we can test
      # that the phase structure is correct for error handling

      recovery_attempt = create_recovery_attempt(TestCluster, 1, {0, 100})

      # The actual execution will fail because TestCluster.otp_name(:sup)
      # doesn't point to a real supervisor, but now it should return an error
      # instead of exiting thanks to our try-catch fix
      assert {%{sequencer: nil}, {:error, {:failed_to_start, :sequencer, _, {:supervisor_exit, _}}}} =
               SequencerStartupPhase.execute(recovery_attempt, %{node_tracking: nil})
    end
  end

  describe "execute/1 with mocked starter functions" do
    test "transitions to next phase when sequencer starts successfully" do
      agent = create_capture_agent()
      sequencer_pid = spawn(fn -> :ok end)

      start_supervised_fn = fn child_spec, node ->
        Agent.update(agent, fn _ -> {child_spec, node} end)
        {:ok, sequencer_pid}
      end

      recovery_attempt = create_recovery_attempt(TestCluster, 42, {10, 100})
      context = %{start_supervised_fn: start_supervised_fn}

      # Should transition to MaterializerBootstrapPhase with sequencer set
      assert {%{sequencer: ^sequencer_pid}, MaterializerBootstrapPhase} =
               SequencerStartupPhase.execute(recovery_attempt, context)

      # Verify the child spec passed to start_supervised_fn
      assert {
               %{
                 id: {Server, TestCluster, 42},
                 start: {GenServer, :start_link, [Server, {_director, 42, 100}, [name: :test_sequencer]]}
               },
               captured_node
             } = Agent.get(agent, & &1)

      # Should be called on current node
      assert captured_node == node()
    end

    test "returns error when starter function fails" do
      start_supervised_fn = fn _child_spec, _node ->
        {:error, :startup_failed}
      end

      recovery_attempt = create_recovery_attempt(TestCluster, 1, {0, 100})
      context = %{start_supervised_fn: start_supervised_fn}

      assert {%{sequencer: nil}, {:error, {:failed_to_start, :sequencer, _, :startup_failed}}} =
               SequencerStartupPhase.execute(recovery_attempt, context)
    end

    test "uses correct last committed version from version vector" do
      agent = create_capture_agent()

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn _ -> child_spec end)
        {:ok, spawn(fn -> :ok end)}
      end

      recovery_attempt = create_recovery_attempt(TestCluster, 5, {25, 250})
      context = %{start_supervised_fn: start_supervised_fn}

      {_result, _next_phase} = SequencerStartupPhase.execute(recovery_attempt, context)

      # Verify epoch 5 and last_committed_version 250 are used
      assert %{start: {GenServer, :start_link, [_, {_director, 5, 250}, _]}} =
               Agent.get(agent, & &1)
    end

    test "starts sequencer at materializer current version when it is ahead of logs" do
      agent = create_capture_agent()

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn _ -> child_spec end)
        {:ok, spawn(fn -> :ok end)}
      end

      recovery_attempt = create_recovery_attempt(TestCluster, 5, {25, 250})

      context = %{
        start_supervised_fn: start_supervised_fn,
        available_services: %{
          "otqxhlks" => {{:materializer, 0}, {:test_materializer, node()}}
        },
        materializer_info_fn: fn _ref, _facts ->
          {:ok, %{current_version: 5_000, durable_version: 5_000}}
        end
      }

      {_result, _next_phase} = SequencerStartupPhase.execute(recovery_attempt, context)

      assert %{start: {GenServer, :start_link, [_, {_director, 5, 5_000}, _]}} =
               Agent.get(agent, & &1)
    end

    test "advances recruited logs to the sequencer start version when materializers are ahead" do
      agent = create_capture_agent()
      log_pid = spawn(fn -> :ok end)

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn _ -> child_spec end)
        {:ok, spawn(fn -> :ok end)}
      end

      {:ok, advanced} = Agent.start_link(fn -> [] end)

      advance_log_fn = fn log_id, pid, target ->
        Agent.update(advanced, fn calls -> [{log_id, pid, target} | calls] end)
        :ok
      end

      recovery_attempt =
        TestCluster
        |> create_recovery_attempt(5, {25, 250})
        |> Map.put(:logs, %{"log-1" => [], "log-2" => []})
        |> Map.put(:service_pids, %{"log-1" => log_pid, "log-2" => log_pid})

      context = %{
        start_supervised_fn: start_supervised_fn,
        advance_log_fn: advance_log_fn,
        available_services: %{
          "otqxhlks" => {{:materializer, 0}, {:test_materializer, node()}}
        },
        materializer_info_fn: fn _ref, _facts ->
          {:ok, %{current_version: 5_000, durable_version: 5_000}}
        end
      }

      assert {%{sequencer: sequencer}, MaterializerBootstrapPhase} =
               SequencerStartupPhase.execute(recovery_attempt, context)

      assert is_pid(sequencer)

      assert %{start: {GenServer, :start_link, [_, {_director, 5, 5_000}, _]}} =
               Agent.get(agent, & &1)

      calls = Agent.get(advanced, &Enum.reverse/1)
      target = Bedrock.DataPlane.Version.from_integer(5_000)

      assert Enum.sort(calls) ==
               Enum.sort([
                 {"log-1", log_pid, target},
                 {"log-2", log_pid, target}
               ])
    end

    test "stalls recovery when a recruited log cannot be advanced" do
      start_supervised_fn = fn _child_spec, _node ->
        flunk("sequencer must not start when log advance fails")
      end

      recovery_attempt =
        TestCluster
        |> create_recovery_attempt(5, {25, 250})
        |> Map.put(:logs, %{"log-1" => []})
        |> Map.put(:service_pids, %{"log-1" => self()})

      context = %{
        start_supervised_fn: start_supervised_fn,
        advance_log_fn: fn _log_id, _pid, _target -> {:error, :unavailable} end,
        available_services: %{
          "otqxhlks" => {{:materializer, 0}, {:test_materializer, node()}}
        },
        materializer_info_fn: fn _ref, _facts ->
          {:ok, %{current_version: 5_000, durable_version: 5_000}}
        end
      }

      assert {_attempt, {:stalled, {:failed_to_advance_logs, %{"log-1" => :unavailable}}}} =
               SequencerStartupPhase.execute(recovery_attempt, context)
    end
  end
end
