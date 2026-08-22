defmodule Bedrock.ObjectStorage.ListError do
  @moduledoc """
  Raised when a lazy object-storage listing fails while it is being enumerated.

  The `list/3` API remains lazy, so a backend cannot return an error tuple after
  enumeration has started. Raising this typed exception prevents a failed or
  partially fetched listing from being mistaken for an empty successful one.
  """

  defexception [:backend, :prefix, :reason]

  @impl true
  def message(%__MODULE__{backend: backend, prefix: prefix, reason: reason}) do
    "#{inspect(backend)} failed to list #{inspect(prefix)}: #{inspect(reason)}"
  end
end
