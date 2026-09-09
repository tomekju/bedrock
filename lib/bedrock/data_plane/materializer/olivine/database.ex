defmodule Bedrock.DataPlane.Materializer.Olivine.Database do
  @moduledoc """
  Database handle for Olivine storage, combining data and index databases.
  """

  alias Bedrock.DataPlane.Materializer.Olivine.CompactionWriter
  alias Bedrock.DataPlane.Materializer.Olivine.DataDatabase
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.IndexDatabase

  @type t :: {DataDatabase.t(), IndexDatabase.t()}
  @type locator :: DataDatabase.locator()

  @spec open(otp_name :: atom(), file_path :: String.t(), opts :: keyword()) ::
          {:ok, t()} | {:error, :compaction_recovery_required | :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path, opts \\ []) when is_atom(otp_name) and is_list(opts) do
    with :ok <- require_complete_cutover(file_path),
         {:ok, data_db} <- DataDatabase.open(file_path, opts),
         {:ok, index_db} <- IndexDatabase.open(otp_name, file_path) do
      {:ok, {data_db, index_db}}
    end
  end

  # A supervisor restart must not create an empty file over an interrupted swap.
  # Leave both originals and compacted recovery inputs for explicit recovery.
  defp require_complete_cutover(file_path) do
    directory = Path.dirname(file_path)

    if Enum.any?(["data.old", "idx.old"], &File.exists?(Path.join(directory, &1))),
      do: {:error, :compaction_recovery_required},
      else: :ok
  end

  @spec close(t()) :: :ok
  def close({data_db, index_db}) do
    DataDatabase.close(data_db)
    IndexDatabase.close(index_db)
    :ok
  end

  @doc """
  Replace live database files with compacted ones and reopen them in this process.

  The compaction task must already have synced and closed the compact files.
  Both compact files are opened here first. Originals stay open until that
  succeeds, so a missing or invalid second file does not close live FDs or ETS.
  Backups are removed only after the swap completes. A failed rollback is
  returned as `{:swap_rollback_failed, reason, undo_errors}` and leaves
  backups in place.
  """
  @spec adopt_compacted_files(
          t(),
          compact_data_path :: charlist(),
          compact_idx_path :: charlist(),
          data_offset :: non_neg_integer(),
          idx_offset :: non_neg_integer(),
          durable_version :: Bedrock.version(),
          opts :: keyword()
        ) :: {:ok, t()} | {:error, term()}
  def adopt_compacted_files(
        {data_db, index_db},
        compact_data_path,
        compact_idx_path,
        data_offset,
        idx_offset,
        durable_version,
        opts \\ []
      ) do
    data_path = data_db.file_name
    idx_path = index_db.file_name
    window_size_in_microseconds = data_db.window_size_in_microseconds
    old_data_path = data_path ++ ~c".old"
    old_idx_path = idx_path ++ ~c".old"
    rename = Keyword.get(opts, :rename, &:file.rename/2)

    case open_compact_pair(
           compact_data_path,
           compact_idx_path,
           data_offset,
           idx_offset,
           window_size_in_microseconds,
           durable_version
         ) do
      {:error, reason} ->
        {:error, reason}

      {:ok, new_data_db, new_index_db} ->
        case swap_compact_paths(
               data_path,
               idx_path,
               compact_data_path,
               compact_idx_path,
               old_data_path,
               old_idx_path,
               rename
             ) do
          :ok ->
            DataDatabase.close(data_db)
            IndexDatabase.close(index_db)
            _ = :file.delete(old_data_path)
            _ = :file.delete(old_idx_path)
            {:ok, {%{new_data_db | file_name: data_path}, %{new_index_db | file_name: idx_path}}}

          {:error, reason} ->
            DataDatabase.close(new_data_db)
            IndexDatabase.close(new_index_db)
            {:error, reason}
        end
    end
  end

  defp open_compact_pair(
         compact_data_path,
         compact_idx_path,
         data_offset,
         idx_offset,
         window_size_in_microseconds,
         durable_version
       ) do
    case DataDatabase.open_existing(compact_data_path, data_offset, window_size_in_microseconds) do
      {:ok, new_data_db} ->
        case IndexDatabase.open_existing(compact_idx_path, idx_offset, durable_version) do
          {:ok, new_index_db} ->
            {:ok, new_data_db, new_index_db}

          {:error, reason} ->
            DataDatabase.close(new_data_db)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp swap_compact_paths(
         data_path,
         idx_path,
         compact_data_path,
         compact_idx_path,
         old_data_path,
         old_idx_path,
         rename
       ) do
    rename_all(
      [
        {data_path, old_data_path},
        {idx_path, old_idx_path},
        {compact_data_path, data_path},
        {compact_idx_path, idx_path}
      ],
      [],
      rename
    )
  end

  defp rename_all([], _done, _rename), do: :ok

  defp rename_all([{from, to} | rest], done, rename) do
    case rename.(from, to) do
      :ok ->
        rename_all(rest, [{to, from} | done], rename)

      {:error, reason} ->
        case undo_renames(done, rename) do
          :ok -> {:error, {:swap_failed, reason}}
          {:error, undo_errors} -> {:error, {:swap_rollback_failed, reason, undo_errors}}
        end
    end
  end

  defp undo_renames(done, rename) do
    errors =
      Enum.reduce(done, [], fn {current, previous}, acc ->
        case rename.(current, previous) do
          :ok -> acc
          {:error, reason} -> [{current, previous, reason} | acc]
        end
      end)

    case errors do
      [] -> :ok
      errors -> {:error, Enum.reverse(errors)}
    end
  end

  @spec load_value(t(), locator()) :: {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value({data_db, _index_db}, locator), do: DataDatabase.load_value(data_db, locator)

  @doc """
  Store a value in the lookaside buffer for the given version and key.
  This is used during transaction application for values within the window.
  """
  @spec store_value(t(), key :: Bedrock.key(), version :: Bedrock.version(), value :: Bedrock.value()) ::
          {:ok, locator(), database :: t()}
  def store_value({data_db, index_db}, key, version, value) do
    {:ok, locator, updated_data_db} = DataDatabase.store_value(data_db, key, version, value)
    {:ok, locator, {updated_data_db, index_db}}
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire Database struct.
  """
  @spec value_loader(t()) :: (locator() -> {:ok, Bedrock.value()} | {:error, :not_found} | {:error, :shutting_down})
  def value_loader({data_db, _index_db}), do: DataDatabase.value_loader(data_db)

  @spec many_value_loader(t()) ::
          ([locator()] ->
             {:ok, %{locator() => Bedrock.value()}}
             | {:error, :not_found}
             | {:error, :shutting_down})
  def many_value_loader({data_db, _index_db}), do: DataDatabase.many_value_loader(data_db)

  @spec durable_version(t()) :: Bedrock.version()
  def durable_version({_data_db, index_db}), do: IndexDatabase.durable_version(index_db)

  @doc """
  Load durable version directly from storage.
  This is useful for background processes that may have a stale database struct.
  """
  @spec load_current_durable_version(t()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_current_durable_version({_data_db, index_db}), do: IndexDatabase.load_durable_version(index_db)

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) :: any() | :undefined
  def info({_data_db, index_db}, stat), do: IndexDatabase.info(index_db, stat)

  @spec advance_durable_version(
          t(),
          version :: Bedrock.version(),
          previous_durable_version :: Bedrock.version(),
          data_size_in_bytes :: pos_integer(),
          collected_pages :: [%{Page.id() => {Page.t(), Page.id()}}]
        ) ::
          {:ok, t(), metadata :: map()}
          | {:error, {:data_flush_failed, term()}}
          | {:error, {:index_flush_failed, term()}}
  def advance_durable_version(
        {data_db, index_db},
        version,
        previous_durable_version,
        data_size_in_bytes,
        collected_pages
      ) do
    start_time = System.monotonic_time(:microsecond)

    {write_time_μs, data_flush_result} = :timer.tc(fn -> DataDatabase.flush(data_db, data_size_in_bytes) end)

    case data_flush_result do
      {:ok, updated_data_db} ->
        {insert_time_μs, index_flush_result} =
          :timer.tc(fn -> IndexDatabase.flush(index_db, version, previous_durable_version, collected_pages) end)

        case index_flush_result do
          {:ok, updated_index_db} ->
            total_duration_μs = System.monotonic_time(:microsecond) - start_time

            metadata = %{
              insert_time_μs: insert_time_μs,
              write_time_μs: write_time_μs,
              total_duration_μs: total_duration_μs
            }

            {:ok, {updated_data_db, updated_index_db}, metadata}

          {:error, reason} ->
            {:error, {:index_flush_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:data_flush_failed, reason}}
    end
  end

  @doc """
  Compacts the database files by building new files with sequential data layout.

  Accepts a writer module and writer state for pluggable output format.
  Returns the writer result, compacted pages, and durable version.

  This is run in a background task and should not block normal operations.
  """
  @spec compact(
          t(),
          complete_page_map :: %{Page.id() => {Page.t(), Page.id()}},
          writer_module :: module(),
          writer :: CompactionWriter.t()
        ) ::
          {:ok, CompactionWriter.result(), compacted_pages :: %{Page.id() => {Page.t(), Page.id()}},
           durable_version :: Bedrock.version()}
          | {:error, term()}
  def compact({data_db, index_db}, complete_page_map, writer_module, writer) do
    durable_version = IndexDatabase.durable_version(index_db)

    # Build compacted data, updating writer state
    {compacted_pages, writer} = build_compacted_data(data_db, complete_page_map, writer_module, writer)

    # Write snapshot index record
    index_record = IndexDatabase.build_snapshot_record(durable_version, compacted_pages)

    with {:ok, writer} <- writer_module.write_index(writer, index_record),
         {:ok, result} <- writer_module.finish(writer) do
      {:ok, result, compacted_pages, durable_version}
    end
  end

  # Build compacted data by iterating pages in key order
  @spec build_compacted_data(
          DataDatabase.t(),
          %{Page.id() => {Page.t(), Page.id()}},
          module(),
          CompactionWriter.t()
        ) :: {%{Page.id() => {Page.t(), Page.id()}}, CompactionWriter.t()}
  defp build_compacted_data(data_db, page_map, writer_module, writer) do
    # Sort pages by their first key for better read locality
    sorted_pages =
      Enum.sort_by(page_map, fn {_id, {page, _next}} ->
        Page.left_key(page) || <<>>
      end)

    # Process each page, accumulating writes
    sorted_pages
    |> Enum.reduce({%{}, 0, writer}, fn {page_id, {page, next_id}}, {pages_acc, offset, w} ->
      # Process all keys in this page
      {new_kvs, new_offset, updated_writer} =
        page
        |> Page.key_locators()
        |> Enum.reduce({[], offset, w}, fn {key, old_locator}, {kvs_acc, current_offset, wr} ->
          # Load value from old location (buffer or disk)
          {:ok, value} = DataDatabase.load_value(data_db, old_locator)

          # Write to compacted output
          {:ok, wr} = writer_module.write_data(wr, value)

          # Create new locator for compacted position
          size = byte_size(value)
          new_locator = <<current_offset::47, size::17>>

          {[{key, new_locator} | kvs_acc], current_offset + size, wr}
        end)

      # Build page with new locators
      compacted_page = Page.new(page_id, Enum.reverse(new_kvs))

      {Map.put(pages_acc, page_id, {compacted_page, next_id}), new_offset, updated_writer}
    end)
    |> then(fn {pages, _offset, writer} -> {pages, writer} end)
  end
end
