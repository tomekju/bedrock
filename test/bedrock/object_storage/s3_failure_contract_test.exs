defmodule Bedrock.ObjectStorage.S3FailureContractTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.HttpClientMock
  alias Bedrock.ObjectStorage.ListError
  alias Bedrock.ObjectStorage.S3
  alias Bedrock.ObjectStorage.Snapshot

  @moduletag :tmp_dir

  setup :verify_on_exit!

  test "S3 listing failures raise a typed lazy-enumeration error" do
    expect_outage(1)
    backend = backend()

    error =
      assert_raise ListError, fn ->
        backend |> ObjectStorage.list("shards/a/") |> Enum.to_list()
      end

    assert error.backend == S3
    assert error.prefix == "shards/a/"
    assert error.reason == :object_store_outage
  end

  test "snapshot discovery and Olivine fail closed when S3 listing fails", %{tmp_dir: tmp_dir} do
    expect_outage(3)
    snapshot = Snapshot.new(backend(), "a")

    assert {:error, {:list_failed, :object_store_outage}} = Snapshot.read_latest(snapshot)
    assert {:error, {:list_failed, :object_store_outage}} = Snapshot.latest_version(snapshot)

    assert {:error, {:snapshot_load_failed, {:list_failed, :object_store_outage}}} =
             Logic.maybe_load_snapshot(tmp_dir, snapshot)

    refute File.exists?(Path.join(tmp_dir, "data"))
    refute File.exists?(Path.join(tmp_dir, "idx"))
  end

  test "chunk discovery cannot turn an S3 listing failure into an empty replay" do
    expect_outage(2)
    reader = ChunkReader.new(backend(), "a")

    assert_raise ListError, fn -> ChunkReader.latest_version(reader) end
    assert_raise ListError, fn -> reader |> ChunkReader.read_all_transactions() |> Enum.to_list() end
  end

  test "HTTP 409 remains a retryable conditional conflict" do
    expect(HttpClientMock, :request, 3, fn
      :head, _url, _body, _headers, _opts ->
        {:ok, %{status_code: 200, headers: [{"etag", "\"current\""}], body: ""}}

      :put, _url, _body, _headers, _opts ->
        {:ok, %{status_code: 409, headers: [], body: "conditional request conflict"}}
    end)

    backend = backend()

    assert {:error, {:conditional_request_conflict, %{status_code: 409}}} =
             ObjectStorage.put_if_not_exists(backend, "conditional/new", "value")

    assert {:error, {:conditional_request_conflict, %{status_code: 409}}} =
             ObjectStorage.put_if_version_matches(
               backend,
               "conditional/current",
               "\"current\"",
               "updated"
             )
  end

  defp expect_outage(count) do
    expect(HttpClientMock, :request, count, fn _method, _url, _body, _headers, _opts ->
      {:error, %{reason: :object_store_outage}}
    end)
  end

  defp backend do
    ObjectStorage.backend(S3,
      bucket: "bedrock-test",
      config: [
        access_key_id: "test-access-key",
        secret_access_key: "test-secret-key",
        region: "auto",
        scheme: "https://",
        host: "t3.storage.dev",
        port: 443,
        http_client: HttpClientMock,
        retries: [max_attempts: 1, base_backoff_in_ms: 1, max_backoff_in_ms: 1]
      ]
    )
  end
end
