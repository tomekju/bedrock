defmodule Bedrock.ObjectStorage.TigrisLiveTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.S3
  alias Bedrock.ObjectStorage.Snapshot
  alias ExAws.Request.Req

  @required_s3_envs ~w(
    BEDROCK_S3_BUCKET
    BEDROCK_S3_ACCESS_KEY_ID
    BEDROCK_S3_SECRET_ACCESS_KEY
    BEDROCK_S3_REGION
    BEDROCK_S3_ENDPOINT
  )
  @tigris_hosts ["t3.storage.dev", "fly.storage.tigris.dev"]
  @cleanup_authorization "delete-created-test-keys"

  @moduletag :external
  @moduletag :tigris
  @moduletag :tmp_dir
  @moduletag timeout: 120_000

  if System.get_env("BEDROCK_LIVE_TIGRIS") != "1" do
    @moduletag skip: "requires BEDROCK_LIVE_TIGRIS=1; enabled runs validate credentials and cleanup authorization"
  end

  setup %{tmp_dir: tmp_dir} do
    require_cleanup_authorization!()
    backend = ObjectStorage.backend(S3, s3_config())
    {_backend_module, backend_config} = backend
    request_config = Keyword.fetch!(backend_config, :config)

    assert Keyword.fetch!(request_config, :http_client) == Req

    run_id = 18 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
    shard_tag = "tigris-live-#{run_id}"
    prefix = "s/#{shard_tag}/"

    assert_prefix_empty!(backend, prefix)

    tracker_path = Path.join(tmp_dir, "created_keys")
    File.write!(tracker_path, "")

    on_exit(fn ->
      tracker_path
      |> read_created_keys()
      |> cleanup(backend, prefix)
    end)

    {:ok, backend: backend, tracker_path: tracker_path, prefix: prefix, shard_tag: shard_tag}
  end

  test "round-trips Tigris objects, conditional writes, and snapshots", %{
    backend: backend,
    prefix: prefix,
    shard_tag: shard_tag,
    tracker_path: tracker_path
  } do
    object_key = prefix <> "object"
    object_data = "object-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}"

    track_created_key(tracker_path, object_key)
    :ok = expect_ok(ObjectStorage.put(backend, object_key, object_data), "put object")
    assert ^object_data = expect_data(ObjectStorage.get(backend, object_key), "get object")
    assert [^object_key] = backend |> ObjectStorage.list(prefix) |> Enum.to_list()

    conditional_key = prefix <> "conditional"
    track_created_key(tracker_path, conditional_key)

    :ok =
      expect_ok(
        ObjectStorage.put_if_not_exists(backend, conditional_key, "initial"),
        "conditionally create object"
      )

    assert {:error, :already_exists} =
             ObjectStorage.put_if_not_exists(backend, conditional_key, "must-not-overwrite")

    assert "initial" = expect_data(ObjectStorage.get(backend, conditional_key), "verify conditional create fence")

    {"initial", version_token} =
      expect_versioned_data(
        ObjectStorage.get_with_version(backend, conditional_key),
        "get object version"
      )

    :ok =
      expect_ok(
        ObjectStorage.put_if_version_matches(backend, conditional_key, version_token, "updated"),
        "conditionally update object"
      )

    assert "updated" = expect_data(ObjectStorage.get(backend, conditional_key), "get updated object")

    assert {:error, :version_mismatch} =
             ObjectStorage.put_if_version_matches(
               backend,
               conditional_key,
               version_token,
               "must-not-overwrite"
             )

    assert "updated" = expect_data(ObjectStorage.get(backend, conditional_key), "verify stale version fence")

    snapshot_version = 42
    snapshot_data = :erlang.term_to_binary(%{kind: :tigris_live_gate, value: object_data})
    snapshot = Snapshot.new(backend, shard_tag)
    snapshot_key = Keys.snapshot_path(shard_tag, snapshot_version)

    track_created_key(tracker_path, snapshot_key)
    :ok = expect_ok(Snapshot.write(snapshot, snapshot_version, snapshot_data), "write snapshot")

    assert ^snapshot_data = expect_data(Snapshot.read(snapshot, snapshot_version), "read snapshot")

    assert {^snapshot_version, ^snapshot_data} =
             expect_latest_snapshot(Snapshot.read_latest(snapshot), "read latest snapshot")

    assert [{^snapshot_version, ^snapshot_key}] = snapshot |> Snapshot.list() |> Enum.to_list()
  end

  defp s3_config do
    Enum.each(@required_s3_envs, &fetch_env!/1)

    region = fetch_env!("BEDROCK_S3_REGION")

    if region != "auto" do
      raise "BEDROCK_S3_REGION must be auto for the live Tigris gate"
    end

    request_config =
      [
        access_key_id: fetch_env!("BEDROCK_S3_ACCESS_KEY_ID"),
        secret_access_key: fetch_env!("BEDROCK_S3_SECRET_ACCESS_KEY"),
        region: region
      ] ++ endpoint_config() ++ [http_client: Req]

    [
      bucket: fetch_env!("BEDROCK_S3_BUCKET"),
      config: request_config
    ]
  end

  defp endpoint_config do
    endpoint = fetch_env!("BEDROCK_S3_ENDPOINT")

    case URI.parse(String.trim(endpoint)) do
      %URI{
        scheme: "https",
        host: host,
        port: port,
        path: path,
        query: nil,
        fragment: nil,
        userinfo: nil
      }
      when host in @tigris_hosts and port in [nil, 443] and path in [nil, "", "/"] ->
        [scheme: "https://", host: host, port: port || 443]

      _ ->
        raise "BEDROCK_S3_ENDPOINT must be a canonical HTTPS Tigris endpoint"
    end
  end

  defp fetch_env!(name) do
    case System.fetch_env(name) do
      {:ok, value} when value != "" -> value
      _ -> raise "missing required Tigris environment configuration"
    end
  end

  defp track_created_key(tracker_path, key) do
    File.write!(tracker_path, key <> "\n", [:append])
  end

  defp read_created_keys(tracker_path) do
    tracker_path
    |> File.read!()
    |> String.split("\n", trim: true)
  end

  defp cleanup(created_keys, backend, prefix) do
    require_cleanup_authorization!()

    created_keys = created_keys |> Enum.uniq() |> MapSet.new()
    assert Enum.all?(created_keys, &String.starts_with?(&1, prefix))

    current_keys = list_current_keys!(backend, prefix)
    version_entries = list_version_entries!(backend, prefix)

    assert Enum.all?(current_keys, &MapSet.member?(created_keys, &1))

    assert Enum.all?(version_entries, fn entry ->
             key = Map.fetch!(entry, :key)
             String.starts_with?(key, prefix) and MapSet.member?(created_keys, key)
           end)

    versioned_keys = MapSet.new(version_entries, &Map.fetch!(&1, :key))

    Enum.each(version_entries, &delete_version_entry!(backend, &1))

    current_keys
    |> Enum.reject(&MapSet.member?(versioned_keys, &1))
    |> Enum.each(fn key ->
      :ok = expect_ok(ObjectStorage.delete(backend, key), "delete created object")
    end)

    Enum.each(created_keys, fn key ->
      :ok = expect_not_found(ObjectStorage.get(backend, key), "verify deleted object")
    end)

    assert_prefix_empty!(backend, prefix)
  end

  defp require_cleanup_authorization! do
    if System.get_env("BEDROCK_LIVE_TIGRIS_ALLOW_CLEANUP") != @cleanup_authorization do
      raise "BEDROCK_LIVE_TIGRIS_ALLOW_CLEANUP must explicitly authorize exact test-key cleanup"
    end
  end

  defp assert_prefix_empty!(backend, prefix) do
    assert [] == list_current_keys!(backend, prefix)
    assert [] == list_version_entries!(backend, prefix)
  end

  defp list_current_keys!({S3, config}, prefix) do
    response =
      config
      |> Keyword.fetch!(:bucket)
      |> ExAws.S3.list_objects_v2(prefix: prefix)
      |> request_success!(config, "list current test objects")

    response.body
    |> Map.get(:contents, [])
    |> Enum.map(&Map.fetch!(&1, :key))
  end

  defp list_version_entries!({S3, config}, prefix) do
    response =
      config
      |> Keyword.fetch!(:bucket)
      |> ExAws.S3.list_object_versions(prefix: prefix)
      |> request_success!(config, "list test object versions")

    Map.get(response.body, :versions, []) ++ Map.get(response.body, :delete_markers, [])
  end

  defp delete_version_entry!({S3, config}, entry) do
    key = Map.fetch!(entry, :key)
    version_id = Map.get(entry, :version_id)
    delete_opts = if version_id in [nil, ""], do: [], else: [version_id: version_id]

    config
    |> Keyword.fetch!(:bucket)
    |> ExAws.S3.delete_object(key, delete_opts)
    |> request_success!(config, "delete exact test object version")
  end

  defp request_success!(operation, config, label) do
    request_config = Keyword.fetch!(config, :config)

    case ExAws.request(operation, request_config) do
      {:ok, response} -> response
      {:error, _reason} -> flunk("#{label} failed")
    end
  end

  defp expect_ok(:ok, _operation), do: :ok
  defp expect_ok({:error, _reason}, operation), do: flunk("#{operation} failed")

  defp expect_data({:ok, data}, _operation), do: data
  defp expect_data({:error, _reason}, operation), do: flunk("#{operation} failed")

  defp expect_not_found({:error, :not_found}, _operation), do: :ok
  defp expect_not_found({:ok, _data}, operation), do: flunk("#{operation} failed")
  defp expect_not_found({:error, _reason}, operation), do: flunk("#{operation} failed")

  defp expect_versioned_data({:ok, data, version_token}, _operation), do: {data, version_token}
  defp expect_versioned_data({:error, _reason}, operation), do: flunk("#{operation} failed")

  defp expect_latest_snapshot({:ok, version, data}, _operation), do: {version, data}
  defp expect_latest_snapshot({:error, _reason}, operation), do: flunk("#{operation} failed")
end
