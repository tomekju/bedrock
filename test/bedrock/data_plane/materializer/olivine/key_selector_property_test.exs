defmodule Bedrock.DataPlane.Materializer.Olivine.KeySelectorPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Tree
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.KeySelector

  @moduletag :property

  # Generators

  def key_generator do
    # Generate keys that are valid binary strings
    StreamData.one_of([
      # Simple single-character keys
      ?a..?z |> StreamData.integer() |> StreamData.map(&<<&1>>),
      # Multi-character keys
      ?a..?z |> StreamData.integer() |> StreamData.list_of() |> StreamData.map(&List.to_string(&1)),
      # Keys with numbers
      {
        ?a..?z |> StreamData.integer() |> StreamData.map(&<<&1>>),
        StreamData.integer(0..99)
      }
      |> StreamData.tuple()
      |> StreamData.map(fn {prefix, suffix} -> "#{prefix}#{suffix}" end),
      # Edge case keys
      StreamData.constant(""),
      StreamData.constant("zzzzz")
    ])
  end

  def offset_generator do
    # Generate realistic offsets, including edge cases
    StreamData.one_of([
      # Small offsets (most common)
      StreamData.integer(-10..10),
      # Medium offsets
      StreamData.integer(-100..100),
      # Large offsets (test cross-page scenarios)
      StreamData.integer(-1000..1000),
      # Edge cases
      StreamData.constant(0)
    ])
  end

  def key_selector_generator do
    {key_generator(), offset_generator(), StreamData.integer(1..4)}
    |> StreamData.tuple()
    |> StreamData.map(fn {key, offset, selector_type} ->
      base_selector =
        case selector_type do
          1 -> KeySelector.first_greater_or_equal(key)
          2 -> KeySelector.first_greater_than(key)
          3 -> KeySelector.last_less_or_equal(key)
          4 -> KeySelector.last_less_than(key)
        end

      KeySelector.add(base_selector, offset)
    end)
  end

  def ordered_key_pair_generator do
    # Generate two keys and ensure they are ordered
    {key_generator(), key_generator()}
    |> StreamData.tuple()
    |> StreamData.map(fn {key1, key2} ->
      if key1 <= key2 do
        {key1, key2}
      else
        {key2, key1}
      end
    end)
  end

  def page_data_generator do
    # Generate realistic page data with varying densities
    1..100
    |> StreamData.integer()
    |> StreamData.map(fn key_count ->
      keys = Enum.map(1..key_count, fn i -> "key#{String.pad_leading(to_string(i), 3, "0")}" end)
      versions = List.duplicate(<<0, 0, 0, 0, 0, 0, 0, 1>>, key_count)
      Enum.zip(keys, versions)
    end)
  end

  def multi_page_index_generator do
    # Generate an index with multiple pages
    2..5
    |> StreamData.integer()
    |> StreamData.map(fn page_count ->
      pages =
        for page_id <- 0..(page_count - 1) do
          base_key_num = page_id * 100

          keys =
            Enum.map(1..50, fn i ->
              "page#{page_id}_key#{String.pad_leading(to_string(base_key_num + i), 3, "0")}"
            end)

          versions = List.duplicate(<<0, 0, 0, 0, 0, 0, 0, 1>>, 50)
          key_locators = Enum.zip(keys, versions)
          next_id = if page_id == page_count - 1, do: 0, else: page_id + 1
          page = Page.new(page_id, key_locators)
          {page, next_id}
        end

      create_index_from_pages(pages)
    end)
  end

  defp create_index_from_pages(page_tuples) do
    page_map =
      Map.new(page_tuples, fn {page, next_id} ->
        {Page.id(page), {page, next_id}}
      end)

    tree = Tree.from_page_map(page_map)
    %Index{tree: tree, page_map: page_map}
  end

  # Property Tests

  property "KeySelector resolution matches the independent sorted-key oracle" do
    check all({index, key_selector} <- StreamData.tuple({multi_page_index_generator(), key_selector_generator()})) do
      index_manager = create_index_manager(index)
      sorted = get_all_keys_from_index(index)
      expected = oracle_resolve(sorted, key_selector)
      actual = normalize_page_for_key(IndexManager.page_for_key(index_manager, key_selector, 1))

      assert actual == expected
    end
  end

  property "Forward offset arithmetic - larger offsets give lexicographically later keys" do
    check all(
            {index, key, offset1, offset_delta} <-
              StreamData.tuple({
                multi_page_index_generator(),
                key_generator(),
                StreamData.integer(0..50),
                StreamData.integer(1..50)
              })
          ) do
      offset2 = offset1 + offset_delta
      selector1 = key |> KeySelector.first_greater_or_equal() |> KeySelector.add(offset1)
      selector2 = key |> KeySelector.first_greater_or_equal() |> KeySelector.add(offset2)
      index_manager = create_index_manager(index)
      sorted = get_all_keys_from_index(index)

      actual1 = normalize_page_for_key(IndexManager.page_for_key(index_manager, selector1, 1))
      actual2 = normalize_page_for_key(IndexManager.page_for_key(index_manager, selector2, 1))

      assert actual1 == oracle_resolve(sorted, selector1)
      assert actual2 == oracle_resolve(sorted, selector2)

      case {actual1, actual2} do
        {{:ok, earlier_key}, {:ok, later_key}} -> assert earlier_key <= later_key
        {_, _} -> :ok
      end
    end
  end

  property "Circuit breaker prevents infinite loops" do
    check all(index <- multi_page_index_generator()) do
      extreme_selector = "" |> KeySelector.first_greater_or_equal() |> KeySelector.add(10_000)
      index_manager = create_index_manager(index)
      sorted = get_all_keys_from_index(index)

      assert oracle_resolve(sorted, extreme_selector) == {:error, :not_found}
      assert {:error, :not_found} = IndexManager.page_for_key(index_manager, extreme_selector, 1)
    end
  end

  property "KeySelector offset of 0 resolves to reference key if it exists" do
    check all(
            index <- multi_page_index_generator(),
            all_keys = get_all_keys_from_index(index),
            length(all_keys) > 0
          ) do
      existing_key = Enum.random(all_keys)
      selector = existing_key |> KeySelector.first_greater_or_equal() |> KeySelector.add(0)
      index_manager = create_index_manager(index)

      assert {:ok, ^existing_key, _page} = IndexManager.page_for_key(index_manager, selector, 1)
    end
  end

  property "Range KeySelector results have consistent bounds" do
    check all(
            {index, start_anchor, end_anchor} <-
              {
                multi_page_index_generator(),
                ordered_key_pair_generator()
              }
              |> StreamData.tuple()
              |> StreamData.map(fn {index, {start_anchor, end_anchor}} -> {index, start_anchor, end_anchor} end)
          ) do
      start_selector = KeySelector.first_greater_or_equal(start_anchor)
      end_selector = KeySelector.first_greater_than(end_anchor)
      index_manager = create_index_manager(index)
      sorted = get_all_keys_from_index(index)

      expected =
        expected_range_result(
          oracle_resolve(sorted, start_selector),
          oracle_resolve(sorted, end_selector)
        )

      actual =
        normalize_pages_for_range(IndexManager.pages_for_range(index_manager, start_selector, end_selector, 1))

      assert actual == expected
    end
  end

  # Helper functions

  defp create_index_manager(index), do: %IndexManager{versions: [{1, {index, %{}}}], current_version: 1}

  defp get_all_keys_from_index(%Index{page_map: page_map}) do
    page_map
    |> Map.values()
    |> Enum.flat_map(fn {page, _next_id} -> Page.keys(page) end)
    |> Enum.sort()
    |> Enum.uniq()
  end

  defp oracle_resolve(sorted_keys, %KeySelector{key: key, or_equal: or_equal, offset: offset}) do
    insertion = Enum.find_index(sorted_keys, &(&1 >= key)) || length(sorted_keys)
    found? = insertion < length(sorted_keys) and Enum.at(sorted_keys, insertion) == key
    base = if found? and not or_equal, do: insertion + 1, else: insertion
    target = base + offset

    if target >= 0 and target < length(sorted_keys) do
      {:ok, Enum.at(sorted_keys, target)}
    else
      {:error, :not_found}
    end
  end

  defp normalize_page_for_key({:ok, resolved_key, _page}), do: {:ok, resolved_key}
  defp normalize_page_for_key({:error, reason}), do: {:error, reason}
  defp normalize_page_for_key(other), do: other

  defp expected_range_result({:ok, resolved_start}, {:ok, resolved_end}) when resolved_start <= resolved_end do
    {:ok, {resolved_start, resolved_end}}
  end

  defp expected_range_result({:ok, resolved_start}, {:ok, resolved_end}) when resolved_start > resolved_end do
    {:error, :invalid_range}
  end

  defp expected_range_result(_start_result, _end_result), do: {:error, :not_found}

  defp normalize_pages_for_range({:ok, {resolved_start, resolved_end}, _pages}) do
    {:ok, {resolved_start, resolved_end}}
  end

  defp normalize_pages_for_range({:error, reason}), do: {:error, reason}
  defp normalize_pages_for_range(other), do: other
end
