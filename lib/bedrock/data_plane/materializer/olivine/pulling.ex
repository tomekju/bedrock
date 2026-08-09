defmodule Bedrock.DataPlane.Materializer.Olivine.Pulling do
  @moduledoc false
  import Bedrock.DataPlane.Materializer.Telemetry

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Worker

  require Logger

  @type puller_state :: %{
          start_after: Bedrock.version(),
          worker_id: Worker.id(),
          apply_transactions_fn: ([Transaction.encoded()] -> Bedrock.version()),
          get_durable_version_fn: (-> Bedrock.version()),
          logs: %{Log.id() => LogDescriptor.t()},
          services: %{Worker.id() => ServiceDescriptor.t()},
          failed_logs: %{Log.id() => any()},
          empty_pulls: non_neg_integer(),
          id: integer()
        }

  @spec start_pulling(
          start_after :: Bedrock.version(),
          worker_id :: Worker.id(),
          logs :: %{Log.id() => LogDescriptor.t()},
          services :: %{Worker.id() => ServiceDescriptor.t()},
          apply_transactions_fn :: ([Transaction.encoded()] -> Bedrock.version()),
          get_durable_version_fn :: (-> Bedrock.version())
        ) :: Task.t()
  def start_pulling(start_after, worker_id, logs, services, apply_transactions_fn, get_durable_version_fn) do
    state = %{
      start_after: start_after,
      worker_id: worker_id,
      apply_transactions_fn: apply_transactions_fn,
      get_durable_version_fn: get_durable_version_fn,
      logs: logs,
      services: services,
      failed_logs: %{},
      empty_pulls: 0,
      id: :rand.uniform(1_000_000)
    }

    Logger.debug(
      "Materializer #{worker_id} starting puller after #{inspect(start_after)}; logs=#{inspect(Map.keys(logs))} services=#{inspect(Map.keys(services))}"
    )

    # PATCHED (fuu): materializer puller lifecycle diagnostics.
    Task.async(fn ->
      try do
        long_pull_loop(state)
      rescue
        exception ->
          Logger.error(
            "Materializer #{worker_id} puller crashed: " <>
              Exception.format(:error, exception, __STACKTRACE__)
          )

          reraise exception, __STACKTRACE__
      catch
        kind, reason ->
          Logger.error(
            "Materializer #{worker_id} puller exited: " <>
              Exception.format(kind, reason, __STACKTRACE__)
          )

          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end)
  end

  @spec stop(Task.t()) :: :ok
  def stop(puller) do
    Task.shutdown(puller)
    :ok
  end

  def circuit_breaker_timeout, do: 10_000
  def retry_delay, do: 1_000
  def call_timeout, do: 5_000

  @spec long_pull_loop(puller_state()) :: no_return()
  def long_pull_loop(%{apply_transactions_fn: apply_transactions_fn} = state) do
    case select_log(state) do
      {:ok, {log_id, %{status: {:up, worker_pid}}}} ->
        trace_log_pull_start(state.start_after, state.start_after)
        subscriber_durable_version = state.get_durable_version_fn.()

        Logger.debug(
          "Materializer #{state.worker_id} pulling from #{inspect(log_id)} after #{inspect(state.start_after)}; subscriber_durable_version=#{inspect(subscriber_durable_version)}"
        )

        case Log.pull(worker_pid, state.start_after,
               limit: 100,
               willing_to_wait_in_ms: call_timeout(),
               subscriber: {state.worker_id, subscriber_durable_version}
             ) do
          {:ok, transactions} ->
            trace_log_pull_succeeded(state.start_after, length(transactions))
            # PATCHED (fuu): materializer pull loop diagnostics.
            maybe_log_pull_result(state, log_id, transactions)
            new_state = process_pulled_transactions(state, transactions, apply_transactions_fn)
            long_pull_loop(new_state)

          {:error, reason} ->
            trace_log_pull_failed(state.start_after, reason)

            Logger.debug(
              "Materializer #{state.worker_id} log pull failed from #{inspect(log_id)} after #{inspect(state.start_after)}: #{inspect(reason)}"
            )

            state
            |> mark_log_as_failed(log_id)
            |> long_pull_loop()
        end

      :no_available_logs ->
        ms_to_wait = retry_delay()
        trace_log_pull_circuit_breaker_tripped(state.start_after, ms_to_wait)

        Logger.debug(
          "Materializer #{state.worker_id} has no available logs after #{inspect(state.start_after)}; logs=#{inspect(Map.keys(state.logs))} services=#{inspect(Map.keys(state.services))} failed_logs=#{inspect(Map.keys(state.failed_logs))}"
        )

        :timer.sleep(ms_to_wait)
        long_pull_loop(reset_failed_logs(state))
    end
  end

  # Select a log, excluding those with active circuit breakers
  @spec select_log(puller_state()) ::
          {:ok, {Log.id(), ServiceDescriptor.t()}} | :no_available_logs
  def select_log(%{logs: logs, services: services, failed_logs: failed_logs}) do
    now = System.monotonic_time(:millisecond)

    available_log_services =
      logs
      |> Map.keys()
      |> Enum.filter(fn log_id ->
        case Map.get(failed_logs, log_id) do
          nil -> true
          retry_timestamp -> now >= retry_timestamp
        end
      end)
      |> Enum.map(&{&1, Map.get(services, &1)})
      |> Enum.reject(&is_nil(elem(&1, 1)))
      |> Map.new()

    if Enum.empty?(available_log_services) do
      :no_available_logs
    else
      {:ok, Enum.random(available_log_services)}
    end
  end

  # Mark a server as failed and set a retry timestamp
  @spec mark_log_as_failed(puller_state(), Log.id()) :: puller_state()
  def mark_log_as_failed(state, log_id) do
    now = System.monotonic_time(:millisecond)
    retry_timestamp = now + circuit_breaker_timeout()
    failed_logs = Map.put(state.failed_logs, log_id, retry_timestamp)

    trace_log_marked_as_failed(state.start_after, log_id)

    %{state | failed_logs: failed_logs}
  end

  # Reset all failed logs, clearing the circuit breakers
  @spec reset_failed_logs(puller_state()) :: puller_state()
  def reset_failed_logs(state) do
    trace_log_pull_circuit_breaker_reset(state.start_after)

    %{state | failed_logs: %{}}
  end

  # Process pulled transactions and update state accordingly
  @spec process_pulled_transactions(puller_state(), [Transaction.encoded()], ([Transaction.encoded()] ->
                                                                                Bedrock.version())) :: puller_state()
  defp process_pulled_transactions(state, [], _apply_transactions_fn) do
    # Add small delay to avoid rapid cycling when no transactions are available
    :timer.sleep(50)
    %{state | empty_pulls: state.empty_pulls + 1}
  end

  defp process_pulled_transactions(state, transactions, apply_transactions_fn) do
    next_version = apply_transactions_fn.(transactions)
    %{state | start_after: next_version, empty_pulls: 0}
  end

  defp maybe_log_pull_result(state, log_id, []) do
    if rem(state.empty_pulls, 20) == 0 do
      Logger.debug(
        "Materializer #{state.worker_id} pulled no transactions from #{inspect(log_id)} after #{inspect(state.start_after)}; logs=#{inspect(Map.keys(state.logs))} services=#{inspect(Map.keys(state.services))}"
      )
    end
  end

  defp maybe_log_pull_result(state, log_id, transactions) do
    Logger.debug(
      "Materializer #{state.worker_id} pulled #{length(transactions)} transactions from #{inspect(log_id)} after #{inspect(state.start_after)}"
    )
  end
end
