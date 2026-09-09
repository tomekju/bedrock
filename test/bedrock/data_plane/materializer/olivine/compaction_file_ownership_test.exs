defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionFileOwnershipTest do
  @moduledoc """
  Compaction writes happen in a Task. Raw `:file` descriptors are process-bound,
  so the writer must sync and close, and the materializer must reopen the
  completed files in its own process before post-cutover flush.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.Server
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  defp unique_tmp_dir(prefix) do
    tmp_dir = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)
    tmp_dir
  end

  defp create_transaction(mutations, version_int) do
    encoded =
      Transaction.encode(%{
        mutations: mutations,
        read_conflicts: {nil, []},
        write_conflicts: []
      })

    {:ok, with_version} = Transaction.add_commit_version(encoded, Version.from_integer(version_int))
    with_version
  end

  defp wait_for_health_report(worker_id, pid, timeout \\ 5_000) do
    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^pid}}} -> :ok
    after
      timeout -> flunk("Did not receive health report within #{timeout}ms")
    end
  end

  def compaction_complete_handler(_event, _measurements, _metadata, {test_pid, ref}) do
    send(test_pid, {:compaction_complete, ref})
  end

  defp attach_compaction_complete do
    ref = make_ref()
    handler_id = {__MODULE__, ref}

    :ok =
      :telemetry.attach(
        handler_id,
        [:bedrock, :materializer, :compaction_complete],
        &__MODULE__.compaction_complete_handler/4,
        {self(), ref}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    ref
  end

  defp await_compaction(pid, complete_ref) do
    assert_receive {:compaction_complete, ^complete_ref}, 10_000
    :sys.get_state(pid)
  end

  test "writer-process compact files can be flushed after this process reopens them" do
    tmp_dir = unique_tmp_dir("olivine_ownership_db")
    otp_name = :"ownership_db_#{System.unique_integer([:positive])}"

    {:ok, state} = Logic.startup(otp_name, self(), "ownership-db", tmp_dir)

    old_txn = create_transaction([{:set, "pre", "compacted"}], 6_000_000)
    new_txn = create_transaction([{:set, "live", "window"}], 12_000_000)
    {:ok, state, _} = Logic.apply_transactions(state, [old_txn])
    {:ok, state, _} = Logic.apply_transactions(state, [new_txn])
    assert {:ok, state} = Logic.advance_window(state)

    durable_before = Database.durable_version(state.database)
    assert durable_before == Version.from_integer(6_000_000)

    {:ok, task} = Logic.start_compaction(state)

    assert_receive {:compaction_ready, compact_data_path, compact_idx_path, data_offset, idx_offset, _pages,
                    durable_version, _duration, _data_before, _idx_before},
                   10_000

    assert :ok = Task.await(task, 10_000)
    assert durable_version == durable_before

    {:ok, new_db} =
      Database.adopt_compacted_files(
        state.database,
        compact_data_path,
        compact_idx_path,
        data_offset,
        idx_offset,
        durable_version
      )

    {data_db, _index_db} = new_db
    assert data_db.file_offset == data_offset
    {:ok, %File.Stat{size: data_size}} = File.stat(List.to_string(data_db.file_name))
    assert data_size == data_offset

    post_version = Version.from_integer(18_000_000)
    {:ok, locator, new_db} = Database.store_value(new_db, "post", post_version, "after-cutover")
    {data_db, _} = new_db

    assert {:ok, flushed_db, _meta} =
             Database.advance_durable_version(
               new_db,
               post_version,
               durable_version,
               data_db.file_offset,
               [%{}]
             )

    assert Database.durable_version(flushed_db) == post_version
    assert {:ok, "after-cutover"} = Database.load_value(flushed_db, locator)

    Database.close(flushed_db)
  end

  test "post-cutover read, write, and flush run on files owned by the materializer" do
    tmp_dir = unique_tmp_dir("olivine_ownership_server")
    worker_id = "ownership-worker-#{System.unique_integer([:positive])}"
    otp_name = :"olivine_ownership_#{System.unique_integer([:positive])}"

    child_spec = %{
      id: {Server, worker_id},
      start: {GenServer, :start_link, [Server, {otp_name, self(), worker_id, tmp_dir, []}, [name: otp_name]]}
    }

    {:ok, pid} = start_supervised(child_spec)
    wait_for_health_report(worker_id, pid)
    {:ok, ^pid, _info} = GenServer.call(pid, {:lock_for_recovery, 1})
    :ok = GenServer.call(pid, {:unlock_after_recovery, Version.zero(), %{logs: %{}, services: %{}}})
    mref = Process.monitor(pid)

    v_old = Version.from_integer(6_000_000)
    v_new = Version.from_integer(12_000_000)

    :ok =
      GenServer.call(
        pid,
        {:ingest,
         [
           create_transaction([{:set, "pre", "compacted"}], 6_000_000),
           create_transaction([{:set, "live", "window"}], 12_000_000)
         ], v_new}
      )

    assert {:ok, "compacted"} = GenServer.call(pid, {:get, "pre", v_old, [wait_ms: 5_000]}, 10_000)
    assert {:ok, "window"} = GenServer.call(pid, {:get, "live", v_new, [wait_ms: 5_000]}, 10_000)

    send(pid, :timeout)
    assert {:ok, ^v_old} = GenServer.call(pid, {:info, :durable_version})

    complete_ref = attach_compaction_complete()
    assert :ok = GenServer.call(pid, :compact)
    state = await_compaction(pid, complete_ref)
    assert is_nil(state.compaction_task)
    assert state.allow_window_advancement
    assert Database.durable_version(state.database) == v_old
    assert {:ok, "compacted"} = GenServer.call(pid, {:get, "pre", v_old, [wait_ms: 0]}, 10_000)

    v_post_old = Version.from_integer(18_000_000)
    v_post_new = Version.from_integer(24_000_000)

    :ok =
      GenServer.call(
        pid,
        {:ingest,
         [
           create_transaction([{:set, "post", "after-cutover"}], 18_000_000),
           create_transaction([{:set, "newer", "flush-me"}], 24_000_000)
         ], v_post_new}
      )

    assert {:ok, "after-cutover"} = GenServer.call(pid, {:get, "post", v_post_old, [wait_ms: 5_000]}, 10_000)
    assert {:ok, "flush-me"} = GenServer.call(pid, {:get, "newer", v_post_new, [wait_ms: 5_000]}, 10_000)
    assert {:ok, "compacted"} = GenServer.call(pid, {:get, "pre", v_post_new, [wait_ms: 0]}, 10_000)

    send(pid, :timeout)
    assert {:ok, ^v_post_old} = GenServer.call(pid, {:info, :durable_version})
    assert {:ok, "after-cutover"} = GenServer.call(pid, {:get, "post", v_post_new, [wait_ms: 0]}, 10_000)
    refute_received {:DOWN, ^mref, :process, ^pid, _}
  end

  test "missing compact idx leaves the original database readable and flushable" do
    {state, compact_data_path, compact_idx_path, data_offset, idx_offset, durable_version} = compact_ready_state()

    File.rm!(to_string(compact_idx_path))

    assert {:error, _} =
             Database.adopt_compacted_files(
               state.database,
               compact_data_path,
               compact_idx_path,
               data_offset,
               idx_offset,
               durable_version
             )

    state = %{state | database: assert_flushable(state.database, "still-open", Version.from_integer(18_000_000), "ok")}
    Logic.shutdown(state)
  end

  test "invalid compact idx leaves the original database readable and flushable" do
    {state, compact_data_path, compact_idx_path, data_offset, idx_offset, durable_version} = compact_ready_state()

    File.write!(to_string(compact_idx_path), "x")

    assert {:error, _} =
             Database.adopt_compacted_files(
               state.database,
               compact_data_path,
               compact_idx_path,
               data_offset,
               idx_offset,
               durable_version
             )

    state = %{state | database: assert_flushable(state.database, "still-open", Version.from_integer(18_000_000), "ok")}
    Logic.shutdown(state)
  end

  test "a failed compact rename restores originals and leaves compact files" do
    {state, compact_data_path, compact_idx_path, data_offset, idx_offset, durable_version} = compact_ready_state()
    {data_db, index_db} = state.database
    data_path = data_db.file_name
    idx_path = index_db.file_name
    old_data_path = data_path ++ ~c".old"
    old_idx_path = idx_path ++ ~c".old"

    rename = fn from, to ->
      if from == compact_idx_path and to == idx_path do
        {:error, :eacces}
      else
        :file.rename(from, to)
      end
    end

    assert {:error, {:swap_failed, :eacces}} =
             Database.adopt_compacted_files(
               state.database,
               compact_data_path,
               compact_idx_path,
               data_offset,
               idx_offset,
               durable_version,
               rename: rename
             )

    assert File.exists?(to_string(data_path))
    assert File.exists?(to_string(idx_path))
    refute File.exists?(to_string(old_data_path))
    refute File.exists?(to_string(old_idx_path))
    assert File.exists?(to_string(compact_data_path))
    assert File.exists?(to_string(compact_idx_path))

    state = %{state | database: assert_flushable(state.database, "rolled-back", Version.from_integer(18_000_000), "ok")}
    Logic.shutdown(state)
  end

  test "a failed rollback keeps backups and compact files and does not restore live serving" do
    {state, compact_data_path, compact_idx_path, data_offset, idx_offset, durable_version} = compact_ready_state()
    {data_db, _index_db} = state.database
    data_path = data_db.file_name
    old_data_path = data_path ++ ~c".old"

    rename = fn from, to ->
      cond do
        from == compact_data_path and to == data_path -> {:error, :eacces}
        from == old_data_path and to == data_path -> {:error, :eperm}
        true -> :file.rename(from, to)
      end
    end

    assert {:error, {:swap_rollback_failed, :eacces, undo_errors}} =
             Database.adopt_compacted_files(
               state.database,
               compact_data_path,
               compact_idx_path,
               data_offset,
               idx_offset,
               durable_version,
               rename: rename
             )

    assert {^old_data_path, ^data_path, :eperm} = List.keyfind(undo_errors, old_data_path, 0)
    assert File.exists?(to_string(old_data_path))
    assert File.exists?(to_string(compact_data_path))
    assert File.exists?(to_string(compact_idx_path))
    refute File.exists?(to_string(data_path))

    Logic.shutdown(state)

    assert {:error, :compaction_recovery_required} =
             Database.open(:rollback_restart, Path.join(Path.dirname(to_string(data_path)), "dets"))

    refute File.exists?(to_string(data_path))
    assert File.exists?(to_string(old_data_path))
  end

  defp compact_ready_state do
    tmp_dir = unique_tmp_dir("olivine_ownership_adopt")
    otp_name = :"ownership_adopt_#{System.unique_integer([:positive])}"
    {:ok, state} = Logic.startup(otp_name, self(), "ownership-adopt", tmp_dir)

    {:ok, state, _} =
      Logic.apply_transactions(state, [create_transaction([{:set, "pre", "compacted"}], 6_000_000)])

    {:ok, state, _} =
      Logic.apply_transactions(state, [create_transaction([{:set, "live", "window"}], 12_000_000)])

    {:ok, state} = Logic.advance_window(state)
    durable_version = Database.durable_version(state.database)

    {:ok, task} = Logic.start_compaction(state)

    assert_receive {:compaction_ready, compact_data_path, compact_idx_path, data_offset, idx_offset, _pages,
                    ^durable_version, _duration, _data_before, _idx_before},
                   10_000

    assert :ok = Task.await(task, 10_000)
    {state, compact_data_path, compact_idx_path, data_offset, idx_offset, durable_version}
  end

  defp assert_flushable(db, key, version, value) do
    previous = Database.durable_version(db)
    {:ok, locator, db} = Database.store_value(db, key, version, value)
    {data_db, _} = db

    assert {:ok, db, _meta} =
             Database.advance_durable_version(db, version, previous, data_db.file_offset, [%{}])

    assert {:ok, ^value} = Database.load_value(db, locator)
    db
  end
end
