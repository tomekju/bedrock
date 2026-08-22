defmodule Bedrock.Cluster.Link.DiscoveryTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster.Link.Discovery
  alias Bedrock.Cluster.Link.Server
  alias Bedrock.Cluster.Link.State

  defmodule RegistrationCluster do
    @moduledoc false

    def otp_name(:foreman), do: :link_discovery_test_foreman
    def gateway_ping_timeout_in_ms, do: 60_000
  end

  defmodule ForemanStub do
    @moduledoc false
    use GenServer

    def start_link(reply) do
      GenServer.start_link(__MODULE__, reply, name: RegistrationCluster.otp_name(:foreman))
    end

    @impl true
    def init(reply), do: {:ok, reply}

    @impl true
    def handle_call(:get_all_running_services, _from, reply), do: {:reply, reply, reply}
  end

  defmodule CoordinatorStub do
    @moduledoc false
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:register_node_resources, client, services, capabilities}, _from, owner) do
      send(owner, {:registered_node_resources, client, services, capabilities})
      {:reply, {:ok, :registered}, owner}
    end
  end

  describe "leader discovery and selection" do
    test "select_leader_from_responses/1 chooses coordinator with highest epoch" do
      responses = [
        {:node1, {:pong, 5, :coordinator_pid_1}},
        {:node2, {:pong, 8, :coordinator_pid_2}},
        {:node3, {:pong, 3, :coordinator_pid_3}}
      ]

      assert {:ok, {:coordinator_pid_2, 8}} =
               Discovery.select_leader_from_responses(responses)
    end

    test "select_leader_from_responses/1 filters out nil leaders" do
      responses = [
        {:node1, {:pong, 5, nil}},
        {:node2, {:pong, 8, :coordinator_pid_2}},
        {:node3, {:pong, 10, nil}}
      ]

      assert {:ok, {:coordinator_pid_2, 8}} =
               Discovery.select_leader_from_responses(responses)
    end

    test "select_leader_from_responses/1 returns error when no leaders available" do
      responses = [
        {:node1, {:pong, 5, nil}},
        {:node2, {:pong, 8, nil}},
        {:node3, {:pong, 3, nil}}
      ]

      assert Discovery.select_leader_from_responses(responses) == {:error, :unavailable}
    end

    test "select_leader_from_responses/1 handles empty responses" do
      assert Discovery.select_leader_from_responses([]) == {:error, :unavailable}
    end

    test "select_leader_from_responses/1 handles single leader" do
      responses = [
        {:node1, {:pong, 5, :coordinator_pid_1}}
      ]

      assert {:ok, {:coordinator_pid_1, 5}} =
               Discovery.select_leader_from_responses(responses)
    end
  end

  describe "change_coordinator/2" do
    setup do
      state = %State{
        node: :test_node,
        cluster: TestCluster,
        known_coordinator: :coordinator_ref,
        transaction_system_layout: %{id: "cached-layout"}
      }

      %{state: state}
    end

    test "does not change when coordinator is the same", %{state: state} do
      assert %State{known_coordinator: :coordinator_ref} =
               Discovery.change_coordinator(state, :coordinator_ref)
    end

    test "sets known_coordinator to unavailable when requested", %{state: state} do
      assert %State{known_coordinator: :unavailable, transaction_system_layout: nil} =
               Discovery.change_coordinator(state, :unavailable)
    end

    test "registers restored services and capabilities together after Foreman is ready" do
      services = [
        {"log-1", :log, :log_1},
        {"materializer-1", :materializer, :materializer_1}
      ]

      start_supervised!({ForemanStub, {:ok, services}})
      coordinator = start_supervised!({CoordinatorStub, self()})

      state = %State{
        cluster: RegistrationCluster,
        capabilities: [:log, :materializer],
        known_coordinator: :unavailable
      }

      assert %State{known_coordinator: ^coordinator} =
               Discovery.change_coordinator(state, coordinator)

      assert_receive {:registered_node_resources, client, ^services, [:log, :materializer]}
      assert client == self()
    end

    test "does not publish empty services when Foreman returns a typed error" do
      start_supervised!({ForemanStub, {:error, :timeout}})
      coordinator = start_supervised!({CoordinatorStub, self()})

      state = %State{
        cluster: RegistrationCluster,
        capabilities: [:log],
        known_coordinator: :unavailable,
        transaction_system_layout: %{id: "cached-layout"}
      }

      assert %State{
               known_coordinator: :unavailable,
               transaction_system_layout: nil,
               timers: %{find_a_live_coordinator: timer_ref}
             } = unavailable_state = Discovery.change_coordinator(state, coordinator)

      assert is_reference(timer_ref)
      refute_receive {:registered_node_resources, _client, _services, _capabilities}

      assert {:noreply, %State{transaction_system_layout: nil}} =
               Server.handle_info({:tsl_updated, %{id: "failed-registration-layout"}}, unavailable_state)
    end

    test "stays alive and schedules discovery when Foreman is unavailable" do
      assert Process.whereis(RegistrationCluster.otp_name(:foreman)) == nil
      coordinator = start_supervised!({CoordinatorStub, self()})

      state = %State{
        cluster: RegistrationCluster,
        capabilities: [:materializer],
        known_coordinator: :unavailable
      }

      assert %State{
               known_coordinator: :unavailable,
               timers: %{find_a_live_coordinator: timer_ref}
             } = Discovery.change_coordinator(state, coordinator)

      assert is_reference(timer_ref)
      refute_receive {:registered_node_resources, _client, _services, _capabilities}
    end
  end

  # Note: try_direct_coordinator_call/2 would require proper mocking setup
  # Integration tests would be more appropriate for testing this functionality
end
