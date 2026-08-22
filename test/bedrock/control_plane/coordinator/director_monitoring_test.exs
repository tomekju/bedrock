defmodule Bedrock.ControlPlane.Coordinator.DirectorMonitoringTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State

  defmodule ConfiguredCluster do
    @moduledoc false

    def node_config do
      [
        parameters: %{
          desired_coordinators: 3,
          desired_logs: 3,
          desired_replication_factor: 3
        }
      ]
    end
  end

  # Common test helpers
  defp create_director_pid, do: spawn(fn -> :timer.sleep(100) end)

  defp leader_state(director \\ :unavailable) do
    %State{
      director: director,
      leader_node: Node.self(),
      my_node: Node.self()
    }
  end

  defp non_leader_state(director) do
    %State{
      director: director,
      leader_node: :other_node,
      my_node: Node.self()
    }
  end

  describe "director management" do
    test "fresh config uses the cluster's durability parameters" do
      config = DirectorManagement.fresh_config(ConfiguredCluster, [node()])

      assert config.parameters.desired_coordinators == 3
      assert config.parameters.desired_logs == 3
      assert config.parameters.desired_replication_factor == 3
    end

    test "handle_director_failure processes failure for current director" do
      director_pid = create_director_pid()
      state = leader_state(director_pid)

      # Capture log output to verify warning message
      log_output =
        capture_log(fn ->
          assert %{director: :unavailable} =
                   DirectorManagement.handle_director_failure(state, director_pid, :test_reason)
        end)

      assert log_output =~ "Director #{inspect(director_pid)} failed with reason: :test_reason"
    end

    test "current Director failure stops the leader Coordinator for a higher Raft epoch" do
      director_pid = create_director_pid()

      state = %{
        leader_state(director_pid)
        | leader_startup_state: :leader_ready,
          transaction_system_layout: %{id: "stale-layout"},
          tsl_subscribers: MapSet.new([self()])
      }

      assert {:stop, {:shutdown, {:director_failed, ^director_pid, :component_down}}, updated_state} =
               Server.handle_info(
                 {:DOWN, make_ref(), :process, director_pid, :component_down},
                 state
               )

      assert updated_state.director == :unavailable
      assert updated_state.leader_startup_state == :recovery_failed
      assert updated_state.transaction_system_layout == nil
      assert_received {:tsl_updated, nil}
    end

    test "subscriber failure does not stop the Coordinator" do
      director_pid = create_director_pid()
      subscriber = spawn(fn -> :ok end)

      state = %{
        leader_state(director_pid)
        | transaction_system_layout: %{id: "current-layout"},
          tsl_subscribers: MapSet.new([subscriber])
      }

      assert {:noreply, updated_state} =
               Server.handle_info(
                 {:DOWN, make_ref(), :process, subscriber, :normal},
                 state
               )

      assert updated_state.director == director_pid
      assert updated_state.transaction_system_layout == %{id: "current-layout"}
      refute MapSet.member?(updated_state.tsl_subscribers, subscriber)
    end

    test "handle_director_failure ignores failure from different director" do
      current_director = create_director_pid()
      different_director = create_director_pid()
      state = leader_state(current_director)

      assert ^state =
               DirectorManagement.handle_director_failure(state, different_director, :test_reason)
    end

    test "handle_director_failure does nothing when not leader" do
      director_pid = create_director_pid()
      state = non_leader_state(director_pid)

      assert ^state =
               DirectorManagement.handle_director_failure(state, director_pid, :test_reason)
    end
  end

  describe "director shutdown" do
    test "shutdown_director_if_running does nothing when not leader" do
      director_pid = create_director_pid()
      state = non_leader_state(director_pid)

      assert %{director: ^director_pid} =
               ^state =
               DirectorManagement.shutdown_director_if_running(state)
    end

    test "shutdown_director_if_running does nothing when no director running" do
      state = leader_state()

      assert %{director: :unavailable} =
               ^state =
               DirectorManagement.shutdown_director_if_running(state)
    end

    test "shutdown_director_if_running sets director to unavailable when leader with running director" do
      director_pid = create_director_pid()

      # Start a test supervisor to handle the terminate_child call
      {:ok, test_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

      state = %State{
        cluster: :test_cluster,
        director: director_pid,
        leader_node: Node.self(),
        my_node: Node.self(),
        supervisor_otp_name: test_supervisor
      }

      # The function will try to terminate via supervisor,
      # expect :not_found since director wasn't started by this supervisor
      assert %{director: :unavailable} =
               DirectorManagement.shutdown_director_if_running(state)

      # Clean up
      DynamicSupervisor.stop(test_supervisor)
    end
  end
end
