defmodule Bedrock.Integration.RF1RestartTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.Test.AdmittedLocalFilesystem

  @moduletag :tmp_dir

  @canary_key "rf1-restart-canary"
  @canary_value "durable-canary"

  defmodule RestartCluster do
    @moduledoc false

    use Bedrock.Cluster,
      otp_app: :bedrock,
      name: "rf1_restart_canary"
  end

  defmodule RestartRepo do
    @moduledoc false

    use Bedrock.Repo, cluster: RestartCluster
  end

  test "a fresh RF=1 cluster restarts from its existing bootstrap and serves an acknowledged canary",
       %{
         tmp_dir: tmp_dir
       } do
    attach_recovery_completion_handler()

    first_boot_admission_tracker = start_supervised!({Agent, fn -> 0 end})
    config = cluster_config(tmp_dir, first_boot_admission_tracker)
    original_config = Application.get_env(:bedrock, RestartCluster)

    Application.put_env(:bedrock, RestartCluster, config)

    on_exit(fn ->
      restore_application_config(original_config)
    end)

    first_cluster = start_cluster!()

    assert is_pid(Process.whereis(RestartCluster.otp_name(:director_recovery_task_supervisor)))
    await_runnable_cluster!()

    assert :written =
             RestartRepo.transact(
               fn ->
                 :ok = RestartRepo.put(@canary_key, @canary_value)
                 :written
               end,
               timeout_in_ms: 5_000,
               retry_limit: 5
             )

    assert {:ok, _bootstrap} = ObjectStorage.get(config[:object_storage], "bootstrap")
    assert 1 = Agent.get(first_boot_admission_tracker, & &1)

    stop_cluster!(first_cluster)

    second_cluster = start_cluster!()

    assert is_pid(Process.whereis(RestartCluster.otp_name(:director_recovery_task_supervisor)))
    await_runnable_cluster!()

    assert @canary_value =
             RestartRepo.transact(
               fn -> RestartRepo.get(@canary_key) end,
               timeout_in_ms: 5_000,
               retry_limit: 5
             )

    assert 1 = Agent.get(first_boot_admission_tracker, & &1)

    stop_cluster!(second_cluster)
  end

  defp cluster_config(tmp_dir, first_boot_admission_tracker) do
    working_dir = Path.join(tmp_dir, "working")

    object_storage =
      ObjectStorage.backend(AdmittedLocalFilesystem,
        root: Path.join(tmp_dir, "object_storage"),
        first_boot_admission_tracker: first_boot_admission_tracker
      )

    [
      capabilities: [:coordination, :log, :materializer],
      path_to_descriptor: Path.join(tmp_dir, "cluster.descriptor"),
      object_storage: object_storage,
      coordinator: [path: working_dir],
      log: [path: working_dir, object_storage: object_storage],
      materializer: [path: working_dir, object_storage: object_storage],
      parameters: %{
        desired_coordinators: 1,
        desired_logs: 1,
        desired_replication_factor: 1
      },
      durability_mode: :relaxed
    ]
  end

  defp start_cluster! do
    {:ok, cluster} = Supervisor.start_link([{RestartCluster, []}], strategy: :one_for_one)
    cluster
  end

  defp attach_recovery_completion_handler do
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:bedrock, :recovery, :completed],
        &__MODULE__.handle_recovery_completed/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  @doc false
  def handle_recovery_completed(_event, _measurements, _metadata, test_pid) do
    send(test_pid, :recovery_completed)
  end

  defp await_runnable_cluster! do
    assert_receive :recovery_completed, 10_000

    _ = :sys.get_state(RestartCluster.otp_name(:coordinator), 5_000)
    _ = :sys.get_state(RestartCluster.otp_name(:link), 5_000)

    assert {:ok, _layout} = RestartCluster.fetch_transaction_system_layout()
  end

  defp stop_cluster!(cluster) do
    monitor_ref = Process.monitor(cluster)

    assert :ok = Supervisor.stop(cluster, :normal, 10_000)
    assert_receive {:DOWN, ^monitor_ref, :process, ^cluster, :normal}, 10_000
  end

  defp restore_application_config(nil), do: Application.delete_env(:bedrock, RestartCluster)

  defp restore_application_config(config), do: Application.put_env(:bedrock, RestartCluster, config)
end
