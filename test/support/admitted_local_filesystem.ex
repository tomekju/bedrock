defmodule Bedrock.Test.AdmittedLocalFilesystem do
  @moduledoc false

  @behaviour Bedrock.ObjectStorage

  alias Bedrock.ObjectStorage.LocalFilesystem

  @impl true
  def put(config, key, data, opts \\ []), do: LocalFilesystem.put(config, key, data, opts)

  @impl true
  def get(config, key), do: LocalFilesystem.get(config, key)

  @impl true
  def delete(config, key), do: LocalFilesystem.delete(config, key)

  @impl true
  def list(config, prefix, opts \\ []), do: LocalFilesystem.list(config, prefix, opts)

  @impl true
  def put_if_not_exists(config, key, data, opts \\ []), do: LocalFilesystem.put_if_not_exists(config, key, data, opts)

  @impl true
  def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)

  @impl true
  def put_if_version_matches(config, key, version_token, data, opts \\ []) do
    LocalFilesystem.put_if_version_matches(config, key, version_token, data, opts)
  end

  @impl true
  def first_boot_admission_status(config, _cluster_config) do
    tracker = Keyword.fetch!(config, :first_boot_admission_tracker)
    root = Keyword.fetch!(config, :root)

    keys =
      if File.dir?(root) do
        config
        |> LocalFilesystem.list("", limit: 2)
        |> Enum.take(2)
      else
        []
      end

    case keys do
      [] ->
        Agent.update(tracker, &(&1 + 1))
        {:ok, :admitted}

      keys ->
        {:error, {:first_boot_namespace_not_pristine, keys}}
    end
  end
end
