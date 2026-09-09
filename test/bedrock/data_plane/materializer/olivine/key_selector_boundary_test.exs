defmodule Bedrock.DataPlane.Materializer.Olivine.KeySelectorBoundaryTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Tree
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.KeySelector

  @locator <<0, 0, 0, 0, 0, 0, 0, 1>>
  @single_page ["aa", "cc", "ee", "gg"]
  @prefix_keys ["r/bug_report_attachments/row1", "r/other_collection/row9"]
  @multi_page [
    ["a00", "a01", "a02"],
    ["b00", "b01", "b02"],
    ["c00", "c01", "c02"]
  ]

  defp manager_from_key_groups(key_groups) do
    pages =
      key_groups
      |> Enum.with_index()
      |> Enum.map(fn {keys, page_id} ->
        page = Page.new(page_id, Enum.map(keys, &{&1, @locator}))
        next_id = if page_id == length(key_groups) - 1, do: 0, else: page_id + 1
        {page, next_id}
      end)

    page_map = Map.new(pages, fn {page, next_id} -> {Page.id(page), {page, next_id}} end)
    index = %Index{tree: Tree.from_page_map(page_map), page_map: page_map}
    %IndexManager{versions: [{1, {index, %{}}}], current_version: 1}
  end

  defp manager_from_keys(keys), do: manager_from_key_groups([keys])

  defp all_keys(key_groups), do: List.flatten(key_groups)

  defp named_base_index(sorted, :first_greater_or_equal, key) do
    Enum.find_index(sorted, &(&1 >= key)) || length(sorted)
  end

  defp named_base_index(sorted, :first_greater_than, key) do
    Enum.find_index(sorted, &(&1 > key)) || length(sorted)
  end

  defp named_base_index(sorted, :last_less_or_equal, key) do
    sorted
    |> Enum.with_index()
    |> Enum.reduce(-1, fn {candidate, index}, acc ->
      if candidate <= key, do: index, else: acc
    end)
  end

  defp named_base_index(sorted, :last_less_than, key) do
    sorted
    |> Enum.with_index()
    |> Enum.reduce(-1, fn {candidate, index}, acc ->
      if candidate < key, do: index, else: acc
    end)
  end

  defp oracle(sorted, name, key, offset \\ 0) do
    target = named_base_index(sorted, name, key) + offset

    if target >= 0 and target < length(sorted) do
      {:ok, Enum.at(sorted, target)}
    else
      {:error, :not_found}
    end
  end

  defp constructor(:first_greater_or_equal, key), do: KeySelector.first_greater_or_equal(key)
  defp constructor(:first_greater_than, key), do: KeySelector.first_greater_than(key)
  defp constructor(:last_less_or_equal, key), do: KeySelector.last_less_or_equal(key)
  defp constructor(:last_less_than, key), do: KeySelector.last_less_than(key)

  defp resolve(manager, selector) do
    case IndexManager.page_for_key(manager, selector, 1) do
      {:ok, resolved_key, _page} -> {:ok, resolved_key}
      {:error, :not_found} -> {:error, :not_found}
      other -> other
    end
  end

  defp assert_named(manager, sorted, name, key, offset \\ 0) do
    selector = name |> constructor(key) |> KeySelector.add(offset)
    assert resolve(manager, selector) == oracle(sorted, name, key, offset)
  end

  describe "diagnostic missing exclusive bound" do
    test "last_less_than of an absent prefix end returns the last in-range key" do
      sorted = Enum.sort(@prefix_keys)
      manager = manager_from_keys(sorted)
      range_end = "r/bug_report_attachments0"

      assert_named(manager, sorted, :last_less_than, range_end)
      assert oracle(sorted, :last_less_than, range_end) == {:ok, "r/bug_report_attachments/row1"}

      {:ok, resolved} = resolve(manager, KeySelector.last_less_than(range_end))
      assert resolved < range_end
    end
  end

  describe "empty index" do
    test "every constructor is not_found on an empty page" do
      manager = manager_from_keys([])

      for name <- [:first_greater_or_equal, :first_greater_than, :last_less_or_equal, :last_less_than],
          key <- ["", "a", <<0xFF>>],
          offset <- [-2, -1, 0, 1, 2] do
        assert_named(manager, [], name, key, offset)
      end
    end
  end

  describe "single-page four constructors" do
    setup do
      sorted = Enum.sort(@single_page)
      %{sorted: sorted, manager: manager_from_keys(sorted)}
    end

    test "existing anchors", %{sorted: sorted, manager: manager} do
      for name <- [:first_greater_or_equal, :first_greater_than, :last_less_or_equal, :last_less_than],
          key <- sorted do
        assert_named(manager, sorted, name, key)
      end
    end

    test "absent anchors between, before first, and after last", %{sorted: sorted, manager: manager} do
      anchors = ["", "ab", "bb", "dd", "ff", "hh", "zz"]

      for name <- [:first_greater_or_equal, :first_greater_than, :last_less_or_equal, :last_less_than],
          key <- anchors do
        refute key in sorted
        assert_named(manager, sorted, name, key)
      end
    end

    test "exact-match last_less_or_equal is the key itself", %{sorted: sorted, manager: manager} do
      assert_named(manager, sorted, :last_less_or_equal, "cc")
      assert resolve(manager, KeySelector.last_less_or_equal("cc")) == {:ok, "cc"}
    end

    test "exact-match last_less_than is the previous key", %{sorted: sorted, manager: manager} do
      assert_named(manager, sorted, :last_less_than, "cc")
      assert resolve(manager, KeySelector.last_less_than("cc")) == {:ok, "aa"}
    end

    test "first_greater_or_equal of an existing key is unchanged baseline", %{sorted: sorted, manager: manager} do
      assert_named(manager, sorted, :first_greater_or_equal, "ee")
      assert resolve(manager, KeySelector.first_greater_or_equal("ee")) == {:ok, "ee"}
    end
  end

  describe "offset arithmetic from named constructors" do
    setup do
      sorted = Enum.sort(@single_page)
      %{sorted: sorted, manager: manager_from_keys(sorted)}
    end

    test "positive and negative add/subtract match the sorted-key oracle", %{sorted: sorted, manager: manager} do
      for name <- [:first_greater_or_equal, :first_greater_than, :last_less_or_equal, :last_less_than],
          key <- ["", "aa", "bb", "cc", "zz"],
          offset <- [-4, -2, -1, 0, 1, 2, 4] do
        assert_named(manager, sorted, name, key, offset)
      end
    end

    test "first_greater_than and first_greater_or_equal+1 agree only when the anchor exists", %{
      sorted: sorted,
      manager: manager
    } do
      existing = "cc"
      missing = "dd"

      fgt_existing = resolve(manager, KeySelector.first_greater_than(existing))
      fge1_existing = resolve(manager, existing |> KeySelector.first_greater_or_equal() |> KeySelector.add(1))
      assert fgt_existing == fge1_existing
      assert fgt_existing == oracle(sorted, :first_greater_than, existing)

      fgt_missing = resolve(manager, KeySelector.first_greater_than(missing))
      fge1_missing = resolve(manager, missing |> KeySelector.first_greater_or_equal() |> KeySelector.add(1))
      assert fgt_missing == oracle(sorted, :first_greater_than, missing)
      assert fge1_missing == oracle(sorted, :first_greater_or_equal, missing, 1)
      refute fgt_missing == fge1_missing
    end
  end

  describe "before first and after last" do
    setup do
      sorted = Enum.sort(@single_page)
      %{sorted: sorted, manager: manager_from_keys(sorted)}
    end

    test "selectors before the first key", %{sorted: sorted, manager: manager} do
      assert resolve(manager, KeySelector.first_greater_or_equal("")) == {:ok, "aa"}
      assert resolve(manager, KeySelector.first_greater_than("")) == {:ok, "aa"}
      assert resolve(manager, KeySelector.last_less_than("aa")) == {:error, :not_found}
      assert resolve(manager, KeySelector.last_less_or_equal("")) == {:error, :not_found}
      assert_named(manager, sorted, :last_less_than, "aa")
      assert_named(manager, sorted, :last_less_or_equal, "")
    end

    test "selectors after the last key", %{sorted: sorted, manager: manager} do
      assert resolve(manager, KeySelector.first_greater_or_equal("zz")) == {:error, :not_found}
      assert resolve(manager, KeySelector.first_greater_than("gg")) == {:error, :not_found}
      assert resolve(manager, KeySelector.last_less_than("zz")) == {:ok, "gg"}
      assert resolve(manager, KeySelector.last_less_or_equal("zz")) == {:ok, "gg"}
      assert_named(manager, sorted, :last_less_than, "zz")
      assert_named(manager, sorted, :first_greater_than, "gg")
    end
  end

  describe "multi-page traversal" do
    setup do
      sorted = all_keys(@multi_page)
      %{sorted: sorted, manager: manager_from_key_groups(@multi_page)}
    end

    test "all four constructors at page-boundary keys", %{sorted: sorted, manager: manager} do
      boundary_keys = ["a02", "b00", "b02", "c00"]

      for name <- [:first_greater_or_equal, :first_greater_than, :last_less_or_equal, :last_less_than],
          key <- boundary_keys,
          offset <- [-5, -1, 0, 1, 5] do
        assert_named(manager, sorted, name, key, offset)
      end
    end

    test "backward walk from the first key of a later page", %{sorted: sorted, manager: manager} do
      assert resolve(manager, KeySelector.last_less_than("b00")) == {:ok, "a02"}
      assert resolve(manager, KeySelector.last_less_or_equal("b00") |> KeySelector.add(-1)) == {:ok, "a02"}
      assert_named(manager, sorted, :last_less_than, "b00")
      assert_named(manager, sorted, :last_less_than, "c00", -2)
    end

    test "forward walk from the last key of an earlier page", %{sorted: sorted, manager: manager} do
      assert resolve(manager, KeySelector.first_greater_than("a02")) == {:ok, "b00"}
      assert resolve(manager, KeySelector.first_greater_or_equal("a02") |> KeySelector.add(1)) == {:ok, "b00"}
      assert_named(manager, sorted, :first_greater_than, "a02")
      assert_named(manager, sorted, :first_greater_or_equal, "a00", 5)
    end

    test "offsets that leave the keyspace are not_found", %{sorted: sorted, manager: manager} do
      assert_named(manager, sorted, :first_greater_or_equal, "a00", 100)
      assert_named(manager, sorted, :last_less_than, "c02", -100)

      assert resolve(manager, "a00" |> KeySelector.first_greater_or_equal() |> KeySelector.add(100)) ==
               {:error, :not_found}

      assert resolve(manager, "c02" |> KeySelector.last_less_than() |> KeySelector.add(-100)) ==
               {:error, :not_found}
    end
  end
end
