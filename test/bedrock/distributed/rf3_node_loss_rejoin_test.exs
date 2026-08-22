defmodule Bedrock.Distributed.RF3NodeLossRejoinTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Test.RF3NodeLossSupport, as: NodeSupport

  @moduletag :distributed
  @moduletag :tmp_dir
  @moduletag timeout: 180_000

  @cleanup_timeout 15_000
  @layout_poll_attempts 200
  @layout_poll_interval 25
  @recovery_timeout 60_000
  @rpc_timeout 35_000

  @pre_loss_key "rf3-node-loss/pre-loss-canary"
  @pre_loss_value "acknowledged-before-node-loss"
  @parked_write_key "rf3-node-loss/parked-write-canary"
  @parked_write_value "must-not-commit-with-one-node-down"
  @post_loss_key "rf3-node-loss/post-loss-canary"
  @post_loss_value "committed-after-rf3-recovery"

  test "RF=3 parks after node loss, preserves acknowledged data, and recovers after rejoin",
       %{tmp_dir: tmp_dir} do
    controller_started? = ensure_controller_distribution!()

    on_exit(fn ->
      stop_controller_distribution!(controller_started?)
    end)

    object_storage_root = Path.join(tmp_dir, "shared-object-storage")

    peers =
      for label <- [:one, :two, :three] do
        start_peer!(label, tmp_dir)
      end

    write_descriptors!(peers)

    Enum.each(peers, &start_cluster!(&1, object_storage_root))
    initial_recovery = await_recovery!("initial RF=3 bootstrap", nil)

    initial_layouts = Enum.map(peers, &await_layout_convergence!/1)
    assert_single_layout!(initial_layouts)

    initial_layout = hd(initial_layouts)
    assert_initial_log_topology!(initial_layout, peers)
    drain_recovery_events()

    [first_peer, second_peer, third_peer] = peers
    assert_strict_rf3_config!(first_peer)

    assert :written = rpc!(first_peer.node, NodeSupport, :write, [@pre_loss_key, @pre_loss_value])
    assert @pre_loss_value = rpc!(second_peer.node, NodeSupport, :read, [@pre_loss_key])
    assert @pre_loss_value = rpc!(third_peer.node, NodeSupport, :read, [@pre_loss_key])

    pre_loss_materializers = await_user_shard_facts!(second_peer)
    assert {:ok, leader_node} = rpc!(first_peer.node, NodeSupport, :fetch_coordinator_node, [])
    lost_peer = non_leader_peer_hosting_active_log!(peers, initial_layout, leader_node)
    surviving_peers = Enum.reject(peers, &(&1.node == lost_peer.node))

    stop_peer!(lost_peer)

    loss_stall =
      await_recovery_stall!(
        "recovery after node loss",
        {:new_epoch, initial_recovery.started},
        {:insufficient_nodes, 3, 2}
      )

    Enum.each(surviving_peers, fn peer ->
      assert {:error, parked_error} =
               rpc!(peer.node, NodeSupport, :attempt_write, [
                 @parked_write_key,
                 @parked_write_value
               ])

      assert is_binary(parked_error) and parked_error != ""
    end)

    rejoined_peer = restart_peer!(lost_peer)
    start_cluster!(rejoined_peer, object_storage_root)

    rejoin_recovery = await_recovery!("recovery after node rejoin", {:after_stall, loss_stall})
    assert_loss_recovery_sequence!(rejoin_recovery, initial_layout)

    recovered_peers = surviving_peers ++ [rejoined_peer]

    recovered_layouts =
      Enum.map(recovered_peers, &await_layout_convergence!(&1, rejoin_recovery.started.epoch))

    assert_single_layout!(recovered_layouts)
    recovered_layout = hd(recovered_layouts)
    assert recovered_layout.epoch == rejoin_recovery.started.epoch
    assert recovered_layout.director == rejoin_recovery.started.recovery_process
    refute recovered_layout.id == initial_layout.id
    assert_initial_log_topology!(recovered_layout, recovered_peers)

    assert @pre_loss_value = rpc!(rejoined_peer.node, NodeSupport, :read, [@pre_loss_key])
    assert nil == rpc!(rejoined_peer.node, NodeSupport, :read, [@parked_write_key])
    await_user_shard_current_versions!(rejoined_peer, pre_loss_materializers, :at_least)

    assert :written =
             rpc!(hd(surviving_peers).node, NodeSupport, :write, [
               @post_loss_key,
               @post_loss_value
             ])

    assert @pre_loss_value =
             rpc!(List.last(surviving_peers).node, NodeSupport, :read, [@pre_loss_key])

    assert @post_loss_value = rpc!(rejoined_peer.node, NodeSupport, :read, [@post_loss_key])
    await_user_shard_current_versions!(rejoined_peer, pre_loss_materializers, :advanced)
  end

  defp ensure_controller_distribution! do
    if Node.alive?() do
      false
    else
      controller_name = "bedrock_rf3_gate_#{System.unique_integer([:positive])}"
      assert {:ok, _node} = Node.start(String.to_atom(controller_name), :shortnames)
      true
    end
  end

  defp stop_controller_distribution!(false), do: :ok

  defp stop_controller_distribution!(true) do
    assert :ok = Node.stop()
    refute Node.alive?()
  end

  defp start_peer!(label, tmp_dir) do
    node_root = Path.join(tmp_dir, "node-#{label}")
    descriptor_path = Path.join(node_root, "bedrock.cluster")
    peer_name = peer_name(label)

    File.mkdir_p!(node_root)

    {:ok, peer, node} = :peer.start(%{name: peer_name, args: peer_args()})
    assert :pong = Node.ping(node)

    peer_info = %{
      descriptor_path: descriptor_path,
      node: node,
      node_root: node_root,
      peer: peer,
      peer_name: peer_name
    }

    register_peer_cleanup!(peer_info)
    peer_info
  end

  defp restart_peer!(peer_info) do
    {:ok, peer, node} = :peer.start(%{name: peer_info.peer_name, args: peer_args()})
    assert peer_info.node == node
    assert :pong = Node.ping(node)

    rejoined_peer = %{peer_info | peer: peer}
    register_peer_cleanup!(rejoined_peer)
    rejoined_peer
  end

  defp peer_name(label) do
    unique = System.unique_integer([:positive])
    String.to_atom("bedrock_rf3_#{label}_#{unique}")
  end

  defp peer_args do
    :code.get_path()
    |> Enum.flat_map(fn path -> [~c"-pa", path] end)
    |> Kernel.++([~c"-setcookie", Atom.to_charlist(Node.get_cookie())])
  end

  defp write_descriptors!(peers) do
    descriptor = Descriptor.new(NodeSupport.cluster_name(), Enum.map(peers, & &1.node))

    Enum.each(peers, fn peer ->
      Descriptor.write_to_file!(peer.descriptor_path, descriptor)
    end)
  end

  defp start_cluster!(peer, object_storage_root) do
    assert {:ok, _supervisor} =
             rpc!(peer.node, NodeSupport, :start, [
               self(),
               peer.node_root,
               peer.descriptor_path,
               object_storage_root
             ])
  end

  defp assert_strict_rf3_config!(peer) do
    assert {:ok, config} = rpc!(peer.node, NodeSupport, :fetch_config, [])

    assert config.parameters.desired_coordinators == 3
    assert config.parameters.desired_logs == 3
    assert config.parameters.desired_replication_factor == 3
  end

  defp await_recovery!(phase, previous_recovery) do
    deadline = System.monotonic_time(:millisecond) + @recovery_timeout
    await_recovery_start!(phase, previous_recovery, deadline)
  end

  defp await_recovery_start!(phase, previous_recovery, deadline) do
    receive do
      {:rf3_recovery, %{event: :started} = started} ->
        if recovery_after?(started, previous_recovery) do
          await_recovery_completion!(phase, started, [started], deadline)
        else
          await_recovery_start!(phase, previous_recovery, deadline)
        end

      {:rf3_recovery, _event} ->
        await_recovery_start!(phase, previous_recovery, deadline)
    after
      remaining_timeout(deadline) ->
        flunk("#{phase} did not emit a fresh recovery-started telemetry event")
    end
  end

  defp await_recovery_completion!(
         phase,
         %{node: node, recovery_process: recovery_process, epoch: epoch, attempt: attempt} = started,
         events,
         deadline
       ) do
    receive do
      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt,
         event: :completed
       } = completed} ->
        %{completed: completed, events: Enum.reverse([completed | events]), started: started}

      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt,
         event: :stalled,
         metadata: %{reason: {:insufficient_nodes, 3, available_nodes}}
       }}
      when phase == "initial RF=3 bootstrap" and available_nodes in 0..2 ->
        await_initial_capability_retry!(phase, node, recovery_process, epoch, attempt, deadline)

      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt,
         event: :stalled,
         metadata: %{reason: {:insufficient_nodes, 3, 2}}
       } = stalled}
      when phase == "recovery after node rejoin" ->
        await_recovery_start!(phase, {:after_stall, stalled}, deadline)

      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt,
         event: event
       } = recovery_event}
      when event in [:stalled, :failed] ->
        flunk("#{phase} #{event}: #{inspect(recovery_event.metadata)}")

      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt
       } = recovery_event} ->
        await_recovery_completion!(phase, started, [recovery_event | events], deadline)

      {:rf3_recovery, _event} ->
        await_recovery_completion!(phase, started, events, deadline)
    after
      remaining_timeout(deadline) ->
        flunk("#{phase} did not emit a matching recovery-completed telemetry event")
    end
  end

  defp await_initial_capability_retry!(phase, node, recovery_process, epoch, stalled_attempt, deadline) do
    receive do
      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: attempt,
         event: :started
       } = started}
      when is_integer(attempt) and attempt > stalled_attempt ->
        await_recovery_completion!(phase, started, [started], deadline)

      {:rf3_recovery, _event} ->
        await_initial_capability_retry!(
          phase,
          node,
          recovery_process,
          epoch,
          stalled_attempt,
          deadline
        )
    after
      remaining_timeout(deadline) ->
        flunk("#{phase} did not retry after the expected empty-capability startup stall")
    end
  end

  defp await_recovery_stall!(phase, previous_recovery, expected_reason) do
    deadline = System.monotonic_time(:millisecond) + @recovery_timeout
    await_recovery_start_for_stall!(phase, previous_recovery, expected_reason, deadline)
  end

  defp await_recovery_start_for_stall!(phase, previous_recovery, expected_reason, deadline) do
    receive do
      {:rf3_recovery, %{event: :started} = started} ->
        if recovery_after?(started, previous_recovery) do
          await_matching_recovery_stall!(phase, started, expected_reason, deadline)
        else
          await_recovery_start_for_stall!(phase, previous_recovery, expected_reason, deadline)
        end

      {:rf3_recovery, _event} ->
        await_recovery_start_for_stall!(phase, previous_recovery, expected_reason, deadline)
    after
      remaining_timeout(deadline) ->
        flunk("#{phase} did not emit a fresh recovery-started telemetry event")
    end
  end

  defp await_matching_recovery_stall!(
         phase,
         %{node: node, recovery_process: recovery_process, epoch: epoch, attempt: attempt},
         expected_reason,
         deadline
       ) do
    receive do
      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt,
         event: :stalled
       } = stalled} ->
        assert stalled.metadata.reason == expected_reason
        stalled

      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt,
         event: :completed
       }} ->
        flunk("#{phase} completed despite the strict three-node placement requirement")

      {:rf3_recovery,
       %{
         node: ^node,
         recovery_process: ^recovery_process,
         epoch: ^epoch,
         attempt: ^attempt,
         event: :failed
       } = failed} ->
        flunk("#{phase} failed: #{inspect(failed.metadata)}")

      {:rf3_recovery, _event} ->
        await_matching_recovery_stall!(
          phase,
          %{
            node: node,
            recovery_process: recovery_process,
            epoch: epoch,
            attempt: attempt
          },
          expected_reason,
          deadline
        )
    after
      remaining_timeout(deadline) ->
        flunk("#{phase} did not emit the expected strict-placement stall")
    end
  end

  defp assert_loss_recovery_sequence!(%{events: events, started: started}, initial_layout) do
    assert started.epoch >= initial_layout.epoch
    assert Enum.any?(events, &(&1.event == :suitable_logs_chosen))
    assert Enum.any?(events, &(&1.event == :old_logs_replayed))
    assert Enum.any?(events, &(&1.event == :system_state_persisted))
  end

  defp recovery_after?(_started, nil), do: true

  defp recovery_after?(started, {:new_epoch, previous}) do
    started.epoch > previous.epoch and
      started.recovery_process != previous.recovery_process
  end

  defp recovery_after?(started, {:after_stall, previous}) do
    started.epoch > previous.epoch or
      (started.epoch == previous.epoch and
         started.recovery_process == previous.recovery_process and
         started.attempt > previous.attempt)
  end

  defp await_layout_convergence!(peer, minimum_epoch \\ 0, attempts \\ @layout_poll_attempts)

  defp await_layout_convergence!(peer, minimum_epoch, attempts) when attempts > 0 do
    case rpc!(peer.node, NodeSupport, :fetch_layouts, []) do
      {:ok, %{authoritative: authoritative, link: link}}
      when authoritative.id == link.id and authoritative.epoch == link.epoch and
             authoritative.epoch >= minimum_epoch ->
        authoritative

      {:error, :unavailable} ->
        wait_for_layout_convergence!(peer, minimum_epoch, attempts)

      {:ok, _layouts} ->
        wait_for_layout_convergence!(peer, minimum_epoch, attempts)

      other ->
        flunk("layout convergence returned #{inspect(other)}")
    end
  end

  defp await_layout_convergence!(_peer, _minimum_epoch, 0),
    do: flunk("peer did not receive a coordinator and Link-converged transaction layout")

  defp wait_for_layout_convergence!(peer, minimum_epoch, attempts) do
    receive do
    after
      @layout_poll_interval -> await_layout_convergence!(peer, minimum_epoch, attempts - 1)
    end
  end

  defp assert_single_layout!(layouts) do
    assert 1 == layouts |> Enum.map(&layout_identity/1) |> Enum.uniq() |> length()
  end

  defp layout_identity(layout), do: {layout.id, layout.epoch}

  defp assert_initial_log_topology!(layout, peers) do
    log_nodes = live_log_nodes!(layout)
    expected_nodes = MapSet.new(Enum.map(peers, & &1.node))

    assert log_nodes == expected_nodes
    assert MapSet.size(log_nodes) == 3
  end

  defp live_log_nodes!(layout) do
    layout.logs
    |> Map.keys()
    |> MapSet.new(fn log_id ->
      assert %{kind: :log, last_seen: {_, node}, status: {:up, pid}} =
               Map.fetch!(layout.services, log_id)

      assert is_pid(pid)
      node
    end)
  end

  defp non_leader_peer_hosting_active_log!(peers, layout, leader_node) do
    log_nodes = live_log_nodes!(layout)
    assert MapSet.member?(log_nodes, leader_node)

    [log_node | _] =
      log_nodes
      |> MapSet.delete(leader_node)
      |> MapSet.to_list()

    Enum.find(peers, &(&1.node == log_node)) || flunk("no peer owns an active log")
  end

  defp await_user_shard_facts!(peer), do: await_user_shard_current_versions!(peer, %{}, :at_least)

  defp await_user_shard_current_versions!(peer, previous_facts, expectation, attempts \\ @layout_poll_attempts)

  defp await_user_shard_current_versions!(peer, previous_facts, expectation, attempts) when attempts > 0 do
    case rpc!(peer.node, NodeSupport, :fetch_user_shard_materializer_facts, []) do
      {:ok, facts_by_shard} when map_size(facts_by_shard) > 0 ->
        if user_shard_versions_satisfy?(facts_by_shard, previous_facts, expectation) do
          facts_by_shard
        else
          wait_for_user_shard_facts!(peer, previous_facts, expectation, attempts)
        end

      _other ->
        wait_for_user_shard_facts!(peer, previous_facts, expectation, attempts)
    end
  end

  defp await_user_shard_current_versions!(_peer, _previous_facts, _expectation, 0),
    do: flunk("user-shard materializers never reached the expected public current version")

  defp wait_for_user_shard_facts!(peer, previous_facts, expectation, attempts) do
    receive do
    after
      @layout_poll_interval ->
        await_user_shard_current_versions!(peer, previous_facts, expectation, attempts - 1)
    end
  end

  defp user_shard_versions_satisfy?(facts_by_shard, previous_facts, expectation) do
    # Repo.transact/2 does not expose a committed or read version. Comparing the
    # public materializer current version before and after the acknowledged write
    # is therefore the strongest public, per-shard durability signal available.
    Enum.all?(facts_by_shard, fn {shard_id, facts} ->
      assert facts.kind == :materializer
      assert facts.shard_id == shard_id
      assert is_binary(facts.current_version)
      assert is_binary(facts.durable_version)

      case Map.fetch(previous_facts, shard_id) do
        :error ->
          true

        {:ok, previous} when expectation == :at_least ->
          facts.current_version >= previous.current_version

        {:ok, previous} when expectation == :advanced ->
          facts.current_version > previous.current_version
      end
    end)
  end

  defp drain_recovery_events do
    receive do
      {:rf3_recovery, _event} -> drain_recovery_events()
    after
      0 -> :ok
    end
  end

  defp remaining_timeout(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp stop_peer!(peer_info) do
    peer = peer_info.peer
    monitor_ref = Process.monitor(peer)

    assert :ok = :peer.stop(peer)
    assert_receive {:DOWN, ^monitor_ref, :process, ^peer, _reason}, @cleanup_timeout
    assert :pang = Node.ping(peer_info.node)
  end

  defp register_peer_cleanup!(peer_info) do
    on_exit(fn ->
      if Process.alive?(peer_info.peer) do
        stop_peer!(peer_info)
      end
    end)
  end

  defp rpc!(node, module, function, arguments) do
    case :rpc.call(node, module, function, arguments, @rpc_timeout) do
      {:badrpc, reason} ->
        flunk("RPC #{inspect(module)}.#{function}/#{length(arguments)} failed: #{inspect(reason)}")

      result ->
        result
    end
  end
end
