defmodule Bedrock.ControlPlane.Coordinator.InitializationTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule TestCluster do
    @moduledoc false

    def fetch_coordinator_nodes, do: {:ok, [Node.self()]}
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
    def otp_name(component), do: :"coordinator_initialization_test_#{component}"
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
end
