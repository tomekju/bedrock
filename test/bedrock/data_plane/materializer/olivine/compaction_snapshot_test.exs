defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionSnapshotTest do
  @moduledoc """
  Compaction must snapshot the exact durable version. Newer insert/update/delete
  belong in replay, and cutover allocator/n_keys must come from that page map.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
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

  defp snapshot_keys(pages) do
    pages
    |> Enum.flat_map(fn {_id, {page, _next}} -> Enum.map(Page.key_locators(page), &elem(&1, 0)) end)
    |> Enum.sort()
  end

  defp fetch(state, key, version) do
    case IndexManager.page_for_key(state.index_manager, key, version) do
      {:ok, page} ->
        case Page.locator_for_key(page, key) do
          {:ok, locator} -> Database.load_value(state.database, locator)
          {:error, :not_found} -> {:error, :not_found}
        end

      error ->
        error
    end
  end

  test "compaction does not put a newer key into a durable version 0 snapshot" do
    tmp_dir = unique_tmp_dir("olivine_snapshot_zero")
    otp_name = :"snapshot_zero_#{System.unique_integer([:positive])}"
    {:ok, state} = Logic.startup(otp_name, self(), "snapshot-zero", tmp_dir)

    {:ok, state, current} =
      Logic.apply_transactions(state, [create_transaction([{:set, "future-key", "future-value"}], 1)])

    durable = Database.durable_version(state.database)
    assert durable == Version.zero()
    assert current == Version.from_integer(1)

    {:ok, task} = Logic.start_compaction(state)

    assert_receive {:compaction_ready, data_path, idx_path, _data_offset, _idx_offset, pages, snapshot_version, _, _, _},
                   10_000

    assert :ok = Task.await(task, 10_000)
    assert snapshot_version == durable
    refute "future-key" in snapshot_keys(pages)

    File.rm(to_string(data_path))
    File.rm(to_string(idx_path))
    Logic.shutdown(state)
  end

  test "start_compaction fails closed when the durable version is not in memory" do
    tmp_dir = unique_tmp_dir("olivine_snapshot_missing")
    otp_name = :"snapshot_missing_#{System.unique_integer([:positive])}"
    {:ok, state} = Logic.startup(otp_name, self(), "snapshot-missing", tmp_dir)

    empty_versions = %{state.index_manager | versions: []}
    state_missing = %{state | index_manager: empty_versions}

    assert {:error, :durable_version_unavailable} = Logic.start_compaction(state_missing)
    Logic.shutdown(state)
  end

  test "durable snapshot omits insert/update/delete; replay restores them" do
    tmp_dir = unique_tmp_dir("olivine_snapshot_replay")
    otp_name = :"snapshot_replay_#{System.unique_integer([:positive])}"
    {:ok, state} = Logic.startup(otp_name, self(), "snapshot-replay", tmp_dir)

    {:ok, state, _} =
      Logic.apply_transactions(
        state,
        [create_transaction([{:set, "keep", "keep-val"}, {:set, "changing", "old-val"}], 6_000_000)]
      )

    {:ok, state, _} =
      Logic.apply_transactions(state, [create_transaction([{:set, "window", "w"}], 12_000_000)])

    {:ok, state} = Logic.advance_window(state)
    durable = Database.durable_version(state.database)
    assert durable == Version.from_integer(6_000_000)

    suffix =
      create_transaction(
        [{:set, "inserted", "new-key"}, {:set, "changing", "new-val"}, {:clear, "keep"}],
        18_000_000
      )

    {:ok, state, _} = Logic.apply_transactions(state, [suffix])
    assert {:ok, "new-key"} = fetch(state, "inserted", Version.from_integer(18_000_000))
    assert {:ok, "new-val"} = fetch(state, "changing", Version.from_integer(18_000_000))
    assert {:error, :not_found} = fetch(state, "keep", Version.from_integer(18_000_000))

    {:ok, task} = Logic.start_compaction(state)

    assert_receive {:compaction_ready, compact_data_path, compact_idx_path, data_offset, idx_offset, pages,
                    snapshot_version, _, _, _},
                   10_000

    assert :ok = Task.await(task, 10_000)
    assert snapshot_version == durable
    assert snapshot_keys(pages) == ["changing", "keep"]

    {:ok, new_im} = IndexManager.from_compacted_pages(state.index_manager, pages, snapshot_version)

    {:ok, new_db} =
      Database.adopt_compacted_files(
        state.database,
        compact_data_path,
        compact_idx_path,
        data_offset,
        idx_offset,
        snapshot_version
      )

    state = %{state | database: new_db, index_manager: new_im}
    assert state.index_manager.current_version == durable
    assert {:ok, "old-val"} = fetch(state, "changing", durable)
    assert {:ok, "keep-val"} = fetch(state, "keep", durable)
    assert {:error, :not_found} = fetch(state, "inserted", durable)
    assert {:error, :not_found} = fetch(state, "window", durable)

    {:ok, state, _} = Logic.apply_transactions(state, [suffix])
    replay_version = Version.from_integer(18_000_000)
    assert {:ok, "new-key"} = fetch(state, "inserted", replay_version)
    assert {:ok, "new-val"} = fetch(state, "changing", replay_version)
    assert {:error, :not_found} = fetch(state, "keep", replay_version)

    Logic.shutdown(state)
  end

  test "cutover allocator and n_keys follow the durable page map after a split and suffix clear" do
    tmp_dir = unique_tmp_dir("olivine_snapshot_split")
    otp_name = :"snapshot_split_#{System.unique_integer([:positive])}"
    {:ok, state} = Logic.startup(otp_name, self(), "snapshot-split", tmp_dir)

    split_mutations = for i <- 1..300, do: {:set, "k#{String.pad_leading(Integer.to_string(i), 3, "0")}", "v#{i}"}
    {:ok, state, _} = Logic.apply_transactions(state, [create_transaction(split_mutations, 6_000_000)])
    {:ok, state, _} = Logic.apply_transactions(state, [create_transaction([{:set, "window", "w"}], 12_000_000)])
    {:ok, state} = Logic.advance_window(state)

    durable = Database.durable_version(state.database)
    assert durable == Version.from_integer(6_000_000)
    {:ok, durable_pages} = IndexManager.page_map_for_version(state.index_manager, durable)
    durable_n_keys = Enum.sum(Enum.map(durable_pages, fn {_id, {page, _}} -> Page.key_count(page) end))
    assert map_size(durable_pages) > 1

    nonzero_id =
      durable_pages
      |> Map.keys()
      |> Enum.reject(&(&1 == 0))
      |> List.first()

    assert is_integer(nonzero_id)
    {page, _next} = Map.fetch!(durable_pages, nonzero_id)
    page_keys = Enum.map(Page.key_locators(page), &elem(&1, 0))
    assert page_keys != []

    suffix = create_transaction(Enum.map(page_keys, &{:clear, &1}), 18_000_000)
    {:ok, state, _} = Logic.apply_transactions(state, [suffix])

    current_pages = IndexManager.get_complete_page_map(state.index_manager)
    refute Map.has_key?(current_pages, nonzero_id)
    assert nonzero_id in state.index_manager.id_allocator.free_ids
    assert state.index_manager.n_keys < durable_n_keys

    {:ok, task} = Logic.start_compaction(state)

    assert_receive {:compaction_ready, compact_data_path, compact_idx_path, data_offset, idx_offset, pages,
                    snapshot_version, _, _, _},
                   10_000

    assert :ok = Task.await(task, 10_000)
    assert snapshot_version == durable
    assert Map.has_key?(pages, nonzero_id)

    {:ok, new_im} = IndexManager.from_compacted_pages(state.index_manager, pages, snapshot_version)
    refute nonzero_id in new_im.id_allocator.free_ids
    assert new_im.n_keys == durable_n_keys
    assert new_im.n_keys != state.index_manager.n_keys

    {:ok, new_db} =
      Database.adopt_compacted_files(
        state.database,
        compact_data_path,
        compact_idx_path,
        data_offset,
        idx_offset,
        snapshot_version
      )

    state = %{state | database: new_db, index_manager: new_im}
    {:ok, state, _} = Logic.apply_transactions(state, [suffix])
    refute Map.has_key?(IndexManager.get_complete_page_map(state.index_manager), nonzero_id)
    assert nonzero_id in state.index_manager.id_allocator.free_ids

    Logic.shutdown(state)
  end
end
