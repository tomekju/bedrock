defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionWriter.SplitFile do
  @moduledoc """
  CompactionWriter implementation that writes to two separate files.

  This is the current behavior used for local storage: data goes to a .data file
  and index goes to a separate .idx file.
  """

  @behaviour Bedrock.DataPlane.Materializer.Olivine.CompactionWriter

  @type t :: %__MODULE__{
          data_fd: :file.fd(),
          idx_fd: :file.fd(),
          data_path: charlist(),
          idx_path: charlist(),
          data_offset: non_neg_integer()
        }

  defstruct [:data_fd, :idx_fd, :data_path, :idx_path, data_offset: 0]

  @type result :: %{
          data_path: charlist(),
          idx_path: charlist(),
          data_offset: non_neg_integer(),
          idx_offset: non_neg_integer()
        }

  @doc """
  Create a new SplitFile writer for the given paths.
  """
  @spec new(data_path :: charlist(), idx_path :: charlist()) ::
          {:ok, t()} | {:error, term()}
  def new(data_path, idx_path) do
    case :file.open(data_path, [:write, :raw, :binary]) do
      {:ok, data_fd} ->
        case :file.open(idx_path, [:write, :raw, :binary]) do
          {:ok, idx_fd} ->
            {:ok,
             %__MODULE__{
               data_fd: data_fd,
               idx_fd: idx_fd,
               data_path: data_path,
               idx_path: idx_path,
               data_offset: 0
             }}

          {:error, reason} ->
            _ = :file.close(data_fd)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  @spec write_data(t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_data(%__MODULE__{} = writer, iodata) do
    case :file.write(writer.data_fd, iodata) do
      :ok ->
        size = IO.iodata_length(iodata)
        {:ok, %{writer | data_offset: writer.data_offset + size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  @spec write_index(t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_index(%__MODULE__{} = writer, iodata) do
    case :file.write(writer.idx_fd, iodata) do
      :ok -> {:ok, writer}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  @spec finish(t()) :: {:ok, result()} | {:error, term()}
  def finish(%__MODULE__{} = writer) do
    finish_result =
      with :ok <- :file.sync(writer.data_fd),
           :ok <- :file.sync(writer.idx_fd) do
        :file.position(writer.idx_fd, {:cur, 0})
      end

    close_result = close_pair(writer.data_fd, writer.idx_fd)

    case {finish_result, close_result} do
      {{:ok, idx_offset}, :ok} ->
        {:ok,
         %{
           data_path: writer.data_path,
           idx_path: writer.idx_path,
           data_offset: writer.data_offset,
           idx_offset: idx_offset
         }}

      {{:ok, _idx_offset}, {:error, reason}} ->
        {:error, reason}

      {error, _} ->
        error
    end
  end

  @doc """
  Close writer descriptors without finishing. Safe to call after `finish/1`.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = writer) do
    _ = close_pair(writer.data_fd, writer.idx_fd)
    :ok
  end

  defp close_pair(data_fd, idx_fd) do
    data_result = :file.close(data_fd)
    idx_result = :file.close(idx_fd)

    cond do
      data_result == :ok and idx_result == :ok -> :ok
      match?({:error, _}, data_result) -> data_result
      true -> idx_result
    end
  end
end
