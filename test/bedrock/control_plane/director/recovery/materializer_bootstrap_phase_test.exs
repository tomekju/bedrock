defmodule Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase
  alias Bedrock.DataPlane.Version

  describe "execute/2" do
    test "for fresh cluster, creates default shard layout and materializers" do
      system_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_materializer_pid = spawn(fn -> Process.sleep(:infinity) end)

      # Track which shards we create materializers for
      created_shards = :ets.new(:created_shards, [:bag, :public])

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})

      # Fresh cluster context - no old logs, but with materializer capability
      context =
        [
          old_transaction_system_layout: %{logs: %{}},
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          {:ok, :new_materializer_ref}
        end)
        |> Map.put(:lock_materializer_fn, fn {:materializer, _ref, shard_tag}, _epoch ->
          :ets.insert(created_shards, {:shard, shard_tag})
          # Return different PIDs for different shards
          pid = if shard_tag == 0, do: system_materializer_pid, else: user_materializer_pid
          {:ok, pid}
        end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          # Should have default shard layout for fresh cluster
          assert updated_attempt.shard_layout
          assert is_map(updated_attempt.shard_layout)

          # Default layout has two shards: system and user
          assert map_size(updated_attempt.shard_layout) == 2

          # Should have created materializers for both shards
          assert map_size(updated_attempt.shard_materializers) == 2
          assert Map.has_key?(updated_attempt.shard_materializers, 0)
          assert Map.has_key?(updated_attempt.shard_materializers, 1)

          # metadata_materializer should be the system shard materializer
          assert updated_attempt.metadata_materializer == system_materializer_pid
        end)

      assert log =~ "Fresh cluster detected"

      # Verify both shards were created
      shards = created_shards |> :ets.lookup(:shard) |> Enum.map(fn {:shard, tag} -> tag end)
      assert 0 in shards
      assert 1 in shards

      :ets.delete(created_shards)
    end

    test "stalls when no materializer capable nodes exist" do
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})

      # Existing cluster - has old logs but no materializer in available_services
      # AND no materializer capability in nodes
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            storage: [Node.self()]
            # Note: no :materializer capability
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})

      log =
        capture_log(fn ->
          assert {_attempt, {:stalled, {:materializer_unavailable, :waiting_for_tagged_materializer}}} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)
        end)

      assert log =~ "waiting for tagged shard"
      refute log =~ "System shard materializer not found, creating new one"
    end

    test "uses existing materializer from available_services (legacy format)" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      # Mock context with materializer available and all required functions
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}}
        })
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, _facts ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.metadata_materializer == materializer_pid
          assert updated_attempt.shard_layout
        end)

      assert log =~ "Materializer caught up to version"
    end

    test "uses materializer from available_services (shard-based format)" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      # New format: {kind, ref, shard_id}
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "mat_sys_0" => {:materializer, {:test_materializer, node()}, 0}
        })
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, _facts ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.metadata_materializer == materializer_pid
          assert updated_attempt.shard_layout
        end)

      assert log =~ "Materializer caught up to version"
    end

    test "reuses merge_node_resources tagged kinds when progress cannot be probed" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)
      created = :ets.new(:created_materializers, [:set, :public])
      system_ref = {:bedrock_fuu_worker_t22odrmd, node()}
      user_ref = {:bedrock_fuu_worker_fmzq3nkp, node()}

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "t22odrmd" => {{:materializer, 0}, system_ref},
          "fmzq3nkp" => {{:materializer, 1}, user_ref}
        })
        |> Map.put(:create_worker_fn, fn _foreman, _id, :materializer, _opts ->
          :ets.insert(created, {:created, true})
          flunk("must not create a replacement materializer when tagged services exist")
        end)
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, _facts -> {:error, :noproc} end)
        |> Map.put(:materializer_read_ready_fn, fn _pid, _version -> :ok end)
        |> Map.put(:materializer_force_durable_checkpoint_fn, fn _pid, _version -> :ok end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.metadata_materializer == materializer_pid
          assert updated_attempt.shard_layout
        end)

      refute log =~ "System shard materializer not found, creating new one"
      assert log =~ "Reusing most advanced shard"
      assert :ets.lookup(created, :created) == []
      :ets.delete(created)
    end

    test "reuses 3-tuple shard materializers when progress cannot be probed" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)
      created = :ets.new(:created_materializers_3tuple, [:set, :public])

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "mat_sys_0" => {:materializer, {:test_materializer, node()}, 0},
          "mat_user_1" => {:materializer, {:test_user_materializer, node()}, 1}
        })
        |> Map.put(:create_worker_fn, fn _foreman, _id, :materializer, _opts ->
          :ets.insert(created, {:created, true})
          flunk("must not create a replacement materializer when tagged 3-tuple services exist")
        end)
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, _facts -> {:error, :noproc} end)
        |> Map.put(:materializer_read_ready_fn, fn _pid, _version -> :ok end)
        |> Map.put(:materializer_force_durable_checkpoint_fn, fn _pid, _version -> :ok end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.metadata_materializer == materializer_pid
        end)

      refute log =~ "System shard materializer not found, creating new one"
      assert :ets.lookup(created, :created) == []
      :ets.delete(created)
    end

    test "stalls existing-cluster recovery when tagged materializers have not advertised yet" do
      durable_version = Version.from_integer(100)
      created = :ets.new(:created_on_missing, [:set, :public])

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "vofp3rks" => {:log, {:bedrock_fuu_worker_vofp3rks, node()}}
        })
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          :ets.insert(created, {:created, true})
          flunk("must not create a replacement materializer on existing-cluster recovery")
        end)

      log =
        capture_log(fn ->
          assert {_attempt, {:stalled, {:materializer_unavailable, :waiting_for_tagged_materializer}}} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)
        end)

      assert log =~ "waiting for tagged shard"
      refute log =~ "creating new one"
      assert :ets.lookup(created, :created) == []
      :ets.delete(created)
    end

    test "stalls on catchup timeout" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)
      # Materializer reports lower version, never catches up
      materializer_version = Version.from_integer(50)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}}
        })
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, [:durable_version] ->
          {:ok, %{durable_version: materializer_version}}
        end)
        # Use very short timeout for testing
        |> Map.put(:catchup_timeout_ms, 50)
        |> Map.put(:catchup_poll_interval_ms, 10)

      log =
        capture_log(fn ->
          assert {_attempt, {:stalled, :catchup_timeout}} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)
        end)

      # Should log waiting messages before timing out
      assert log =~ "Materializer at version"
      assert log =~ "waiting for"
    end

    test "stalls on unlock failure" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}}
        })
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl ->
          {:error, :test_unlock_error}
        end)

      # No logs expected - unlock fails immediately
      assert {_attempt, {:stalled, {:unlock_failed, :test_unlock_error}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "stalls on worker creation failure" do
      durable_version = Version.from_integer(100)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => [0, 1]})
        |> Map.put(:durable_version, durable_version)

      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]}
          },
          node_capabilities: %{
            log: [Node.self()],
            materializer: [Node.self()]
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{})
        |> Map.put(:create_worker_fn, fn _foreman_ref, _worker_id, :materializer, _opts ->
          flunk("must not create a replacement materializer on existing-cluster recovery")
        end)

      log =
        capture_log(fn ->
          assert {_attempt, {:stalled, {:materializer_unavailable, :waiting_for_tagged_materializer}}} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)
        end)

      assert log =~ "waiting for tagged shard"
    end

    test "filters logs to only system shard when unlocking" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)
      durable_version = Version.from_integer(100)

      # Logs with different shard assignments
      # log_1 handles shard 0 (system), log_2 handles shard 1 (user)
      logs = %{
        "log_1" => [0],
        "log_2" => [1]
      }

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, logs)
        |> Map.put(:durable_version, durable_version)

      received_tsl = :ets.new(:test_tsl, [:set, :public])

      context =
        [
          old_transaction_system_layout: %{
            logs: logs
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:materializer, {:test_materializer, node()}}
        })
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, tsl ->
          :ets.insert(received_tsl, {:tsl, tsl})
          :ok
        end)
        |> Map.put(:materializer_info_fn, fn _pid, _facts ->
          {:ok, %{durable_version: durable_version}}
        end)
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)

      log =
        capture_log(fn ->
          assert {_updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          # Verify that only system shard logs were passed to unlock
          [{:tsl, tsl}] = :ets.lookup(received_tsl, :tsl)
          assert Map.keys(tsl.logs) == ["log_1"]
          refute Map.has_key?(tsl.logs, "log_2")
        end)

      assert log =~ "Materializer caught up to version"

      :ets.delete(received_tsl)
    end

    test "retries shard layout at materializer version when log version is too old" do
      system_pid = spawn(fn -> Process.sleep(:infinity) end)
      user_pid = spawn(fn -> Process.sleep(:infinity) end)
      log_last = Version.from_integer(100)
      mat_ver = Version.from_integer(5_000)
      layout = %{<<0xFF>> => {1, <<>>}, Bedrock.end_of_keyspace() => {0, <<0xFF>>}}

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)
        |> Map.put(:logs, %{"log_1" => []})
        |> Map.put(:version_vector, {Version.from_integer(0), log_last})
        |> Map.put(:durable_version, Version.from_integer(0))

      queried_versions = :ets.new(:queried_versions, [:bag, :public])

      context =
        [
          old_transaction_system_layout: %{logs: %{"log_1" => []}}
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "otqxhlks" => {{:materializer, 0}, {:sys_mat, node()}},
          "qksv2gvr" => {{:materializer, 1}, {:user_mat, node()}}
        })
        |> Map.put(:lock_materializer_fn, fn service, _epoch ->
          case service do
            {{:materializer, 1}, _} -> {:ok, user_pid}
            _ -> {:ok, system_pid}
          end
        end)
        |> Map.put(:unlock_materializer_fn, fn _pid, _version, _tsl -> :ok end)
        |> Map.put(:materializer_info_fn, fn _pid, _facts ->
          {:ok, %{current_version: mat_ver, durable_version: mat_ver}}
        end)
        |> Map.put(:materializer_read_ready_fn, fn _pid, _version -> :ok end)
        |> Map.put(:materializer_force_durable_checkpoint_fn, fn _pid, _version -> :ok end)
        |> Map.put(:get_shard_layout_fn, fn _pid, version ->
          :ets.insert(queried_versions, {:version, version})

          if version == log_last do
            {:error, {:shard_layout_query_failed, :version_too_old}}
          else
            {:ok, layout}
          end
        end)

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          assert updated_attempt.shard_layout == layout
          assert updated_attempt.metadata_materializer == system_pid
          assert updated_attempt.shard_materializers[0] == system_pid
          assert updated_attempt.shard_materializers[1] == user_pid
        end)

      assert log =~ "retry shard layout at materializer version"

      versions = queried_versions |> :ets.lookup(:version) |> Enum.map(&elem(&1, 1))
      assert log_last in versions
      assert mat_ver in versions

      :ets.delete(queried_versions)
    end
  end

  describe "default_shard_layout/0" do
    test "returns two shards: system and user" do
      layout = MaterializerBootstrapPhase.default_shard_layout()

      assert is_map(layout)
      assert map_size(layout) == 2

      # System shard: "" to 0xFF (tag 1)
      # User shard: 0xFF to end_of_keyspace (tag 0)
      assert Map.has_key?(layout, <<0xFF>>)
      assert Map.has_key?(layout, Bedrock.end_of_keyspace())
    end
  end

  describe "system_shard_id/0" do
    test "returns 0 for the system shard" do
      assert RecoveryAttempt.system_shard_id() == 0
    end
  end
end
