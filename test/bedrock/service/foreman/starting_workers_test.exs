defmodule Bedrock.Service.Foreman.StartingWorkersTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.StartingWorkers
  alias Bedrock.Service.Foreman.StartingWorkers.StartWorkerOp
  alias Bedrock.Service.Foreman.WorkerInfo

  # Define mock modules at compile time
  defmodule MockWorker do
    @moduledoc false
    def child_spec(opts) do
      # Return the opts as the start tuple so we can inspect what was passed
      %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
    end
  end

  defmodule MockCluster do
    @moduledoc false
    def otp_name(:foreman), do: :test_foreman
    def otp_name(:foreman_task_supervisor), do: :test_foreman_task_supervisor
    def otp_name(:worker_supervisor), do: :test_worker_supervisor
  end

  describe "build_child_spec/1" do
    test "includes object_storage in worker options" do
      mock_manifest = %{
        worker: MockWorker,
        params: %{}
      }

      object_storage = {Bedrock.ObjectStorage.LocalFilesystem, root: "/tmp/test"}

      op = %StartWorkerOp{
        id: "test-worker",
        path: "/tmp/workers/test-worker",
        otp_name: :test_worker,
        cluster: MockCluster,
        manifest: mock_manifest,
        object_storage: object_storage,
        error: nil
      }

      result = StartingWorkers.build_child_spec(op)

      # Extract the opts that were passed to the worker's child_spec
      %{start: {_mod, :start_link, [opts]}} = result.child_spec

      assert Keyword.get(opts, :object_storage) == object_storage
    end
  end

  describe "try_to_start_workers/4" do
    test "preserves the original worker when a start task exits" do
      start_supervised!({Task.Supervisor, name: MockCluster.otp_name(:foreman_task_supervisor)})

      workers = [
        %WorkerInfo{
          id: "crashing-worker",
          path: "/tmp/crashing-worker",
          otp_name: :crashing_worker,
          health: :stopped
        },
        %WorkerInfo{
          id: "healthy-worker",
          path: "/tmp/healthy-worker",
          otp_name: :healthy_worker,
          health: :stopped
        }
      ]

      started =
        workers
        |> StartingWorkers.try_to_start_workers(MockCluster, :unused,
          start_worker_fn: fn
            %{id: "crashing-worker"} -> exit(:start_crashed)
            worker -> %{worker | health: {:ok, self()}}
          end
        )
        |> Map.new(&{&1.id, &1})

      assert %{health: {:failed_to_start, :start_crashed}, path: "/tmp/crashing-worker", otp_name: :crashing_worker} =
               started["crashing-worker"]

      assert %{health: {:ok, _pid}} = started["healthy-worker"]
    end

    test "reports a timed-out start against the original worker" do
      start_supervised!({Task.Supervisor, name: MockCluster.otp_name(:foreman_task_supervisor)})

      worker = %WorkerInfo{
        id: "slow-worker",
        path: "/tmp/slow-worker",
        otp_name: :slow_worker,
        health: :stopped
      }

      assert [%{id: "slow-worker", health: {:failed_to_start, :timeout}}] =
               StartingWorkers.try_to_start_workers([worker], MockCluster, :unused,
                 timeout_in_ms: 10,
                 start_worker_fn: fn _worker ->
                   receive do
                     :never -> :ok
                   end
                 end
               )
    end
  end
end
