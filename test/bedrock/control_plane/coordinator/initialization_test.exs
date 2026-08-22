defmodule Bedrock.ControlPlane.Coordinator.InitializationTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap
  alias Bedrock.Test.AdmittedLocalFilesystem

  @moduletag :tmp_dir

  defmodule TestCluster do
    @moduledoc false

    def fetch_coordinator_nodes, do: {:ok, [Node.self()]}
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
    def otp_name(component), do: :"coordinator_initialization_test_#{component}"
  end

  defmodule UnavailableNodesCluster do
    @moduledoc false

    def fetch_coordinator_nodes, do: {:error, :unavailable}
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
    def otp_name(component), do: :"coordinator_unavailable_nodes_test_#{component}"
  end

  defmodule AdmittedFirstBootBackend do
    @moduledoc false

    def get(_config, "bootstrap"), do: {:error, :not_found}
    def first_boot_admission_status(_config, _cluster_config), do: {:ok, :admitted}
  end

  defmodule OutageBackend do
    @moduledoc false

    def get(_config, "bootstrap"), do: {:error, :object_store_outage}
  end

  defmodule StaleFollowerAdmissionBackend do
    @moduledoc false

    def get(_config, "bootstrap"), do: {:error, :not_found}

    def first_boot_admission_status(config, _cluster_config) do
      Agent.get_and_update(Keyword.fetch!(config, :admission_tracker), fn
        :pristine ->
          {{:ok, :admitted}, :namespace_initialized_elsewhere}

        :namespace_initialized_elsewhere ->
          {{:error, :namespace_initialized_elsewhere}, :namespace_initialized_elsewhere}
      end)
    end
  end

  setup %{tmp_dir: tmp_dir} do
    backend = ObjectStorage.backend(LocalFilesystem, root: tmp_dir)
    Application.put_env(:bedrock, TestCluster, object_storage: backend)
    on_exit(fn -> Application.delete_env(:bedrock, TestCluster) end)
    %{backend: backend}
  end

  test "bootstrap logs are recovery input, not a runnable layout", %{backend: backend} do
    bootstrap = %{
      cluster_id: "cluster-1",
      epoch: 7,
      logs: [%{id: "old-log", otp_ref: nil, shard_tags: []}],
      coordinators: [%{node: Atom.to_string(Node.self())}]
    }

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))

    assert {:ok, state, {:continue, :check_recovery_consensus}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})

    assert state.old_transaction_system_layout == %{logs: %{"old-log" => []}}
    assert state.transaction_system_layout == nil
  end

  test "missing bootstrap requires validated first-boot admission" do
    assert {:stop,
            {:bootstrap_load_failed,
             {:bootstrap_missing_without_validated_first_boot_admission, :first_boot_admission_validator_missing}}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})
  end

  test "admitted first boot is the only empty bootstrap state" do
    Application.put_env(:bedrock, TestCluster, object_storage: {AdmittedFirstBootBackend, []})

    assert {:ok, state, {:continue, :check_recovery_consensus}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})

    assert state.epoch == nil
    assert state.config == nil
    assert state.old_transaction_system_layout == nil
  end

  test "uses injected descriptor nodes before Link is available" do
    Application.put_env(:bedrock, UnavailableNodesCluster, object_storage: {AdmittedFirstBootBackend, []})

    on_exit(fn -> Application.delete_env(:bedrock, UnavailableNodesCluster) end)

    assert {:ok, state, {:continue, :check_recovery_consensus}} =
             Server.init({UnavailableNodesCluster, UnavailableNodesCluster.otp_name(:coordinator), [Node.self()]})

    assert state.coordinator_nodes == [Node.self()]
    assert %Bedrock.Raft{} = state.raft
  end

  test "object storage outages stop coordinator initialization" do
    Application.put_env(:bedrock, TestCluster, object_storage: {OutageBackend, []})

    assert {:stop, {:bootstrap_load_failed, {:bootstrap_fetch_failed, :object_store_outage}}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})
  end

  test "malformed bootstrap data stops coordinator initialization", %{backend: backend} do
    :ok = ObjectStorage.put(backend, "bootstrap", "not a cluster bootstrap")

    assert {:stop, {:bootstrap_load_failed, {:bootstrap_parse_failed, _reason}}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})
  end

  test "leadership reloads bootstrap written after pristine coordinator initialization", %{
    tmp_dir: tmp_dir
  } do
    admission_tracker = start_supervised!({Agent, fn -> 0 end})

    backend =
      ObjectStorage.backend(AdmittedLocalFilesystem,
        root: tmp_dir,
        first_boot_admission_tracker: admission_tracker
      )

    Application.put_env(:bedrock, UnavailableNodesCluster, object_storage: backend)

    on_exit(fn -> Application.delete_env(:bedrock, UnavailableNodesCluster) end)

    assert {:ok, fresh_state, {:continue, :check_recovery_consensus}} =
             Server.init({UnavailableNodesCluster, UnavailableNodesCluster.otp_name(:coordinator), [Node.self()]})

    assert fresh_state.config == nil
    assert fresh_state.old_transaction_system_layout == nil

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap(epoch: 7)))

    start_supervised!({Task.Supervisor, name: UnavailableNodesCluster.otp_name(:director_recovery_task_supervisor)})

    director_supervisor = start_supervised!({DynamicSupervisor, strategy: :one_for_one})

    assert {:noreply, leader_state} =
             Server.handle_info(
               {:raft, :leadership_changed, {Node.self(), 11}},
               %{fresh_state | supervisor_otp_name: director_supervisor}
             )

    assert leader_state.epoch == 11
    assert is_pid(leader_state.director)

    assert %{
             epoch: 11,
             config: %{parameters: %{desired_logs: 3}},
             old_transaction_system_layout: %{logs: %{"persisted-log" => []}}
           } = :sys.get_state(leader_state.director)
  end

  test "stale pre-bootstrap follower stops when first-boot admission no longer validates" do
    admission_tracker = start_supervised!({Agent, fn -> :pristine end})

    backend =
      ObjectStorage.backend(StaleFollowerAdmissionBackend, admission_tracker: admission_tracker)

    Application.put_env(:bedrock, TestCluster, object_storage: backend)

    assert {:ok, stale_follower, {:continue, :check_recovery_consensus}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})

    assert {:stop,
            {:leadership_bootstrap_reload_failed,
             {:bootstrap_missing_without_validated_first_boot_admission, :namespace_initialized_elsewhere}},
            stopped_state} =
             Server.handle_info({:raft, :leadership_changed, {Node.self(), 1}}, stale_follower)

    assert stopped_state.director == :unavailable
    assert stopped_state.leader_startup_state == :recovery_failed
  end

  test "leadership stops when persisted bootstrap epoch is ahead of the elected Raft epoch", %{
    backend: backend
  } do
    :ok =
      ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap(epoch: 12)))

    assert {:ok, loaded_state, {:continue, :check_recovery_consensus}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})

    assert {:stop, {:leadership_bootstrap_reload_failed, {:bootstrap_epoch_ahead_of_raft, 12, 11}}, stopped_state} =
             Server.handle_info({:raft, :leadership_changed, {Node.self(), 11}}, loaded_state)

    assert stopped_state.epoch == 11
    assert stopped_state.director == :unavailable
    assert stopped_state.leader_startup_state == :recovery_failed
  end

  defp bootstrap(opts) do
    %{
      cluster_id: "cluster-1",
      epoch: Keyword.fetch!(opts, :epoch),
      logs: [%{id: "persisted-log", otp_ref: nil, shard_tags: []}],
      coordinators: [%{node: Atom.to_string(Node.self())}],
      parameters: %{
        desired_coordinators: 1,
        desired_logs: 3,
        desired_replication_factor: 3,
        desired_commit_proxies: 1,
        desired_read_version_proxies: 1,
        ping_rate_in_hz: 10,
        retransmission_rate_in_hz: 20,
        transaction_window_in_ms: 5_000
      },
      policies: %{allow_volunteer_nodes_to_join: true}
    }
  end
end
