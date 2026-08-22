defmodule Bedrock.Internal.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  @end_of_keyspace <<0xFF, 0xFF>>

  test "normalizes a shared adjacent-shard boundary before building the tree" do
    right_materializer =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> send(right_materializer, :stop) end)

    index =
      LayoutIndex.build_index(%{
        shard_layout: %{
          "m" => {0, ""},
          @end_of_keyspace => {1, "m"}
        },
        metadata_materializer: self(),
        shard_materializers: %{1 => right_materializer}
      })

    assert LayoutIndex.lookup_key!(index, "m") == {{"m", @end_of_keyspace}, [right_materializer]}

    assert LayoutIndex.lookup_range(index, "", @end_of_keyspace) == [
             {{"", "m"}, [self()]},
             {{"m", @end_of_keyspace}, [right_materializer]}
           ]
  end
end
