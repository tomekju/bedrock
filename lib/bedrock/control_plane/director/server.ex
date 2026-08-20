defmodule Bedrock.ControlPlane.Director.Server do
  @moduledoc false

  use GenServer

  import Bedrock.ControlPlane.Director.Nodes,
    only: [
      request_to_rejoin: 5,
      node_added_worker: 4,
      update_last_seen_at: 3,
      update_minimum_read_version: 3,
      ping_all_coordinators: 1,
      request_worker_creation: 4
    ]

  import Bedrock.ControlPlane.Director.Recovery,
    only: [
      try_to_recover: 1,
      persist_config: 1,
      persist_new_transaction_system_layout: 1
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.Service.Worker

  require Logger

  @doc false
  @spec child_spec(
          opts :: [
            cluster: module(),
            config: Config.t(),
            old_transaction_system_layout: TransactionSystemLayout.t() | nil,
            epoch: Bedrock.epoch(),
            coordinator: Coordinator.ref(),
            services: %{String.t() => {atom(), {atom(), node()}}} | nil,
            node_capabilities: %{Bedrock.Cluster.capability() => [node()]} | nil
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster param"
    config = opts[:config] || raise "Missing :config param"
    epoch = opts[:epoch] || raise "Missing :epoch param"
    coordinator = opts[:coordinator] || raise "Missing :coordinator param"
    services = opts[:services] || %{}
    node_capabilities = opts[:node_capabilities] || %{}
    old_transaction_system_layout = opts[:old_transaction_system_layout] || nil

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, config, old_transaction_system_layout, epoch, coordinator, services, node_capabilities}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({cluster, config, old_transaction_system_layout, epoch, coordinator, services, node_capabilities}) do
    state = %State{
      epoch: epoch,
      cluster: cluster,
      config: config,
      old_transaction_system_layout: old_transaction_system_layout,
      coordinator: coordinator,
      node_capabilities: node_capabilities,
      lock_token: :crypto.strong_rand_bytes(32),
      services: services
    }

    {:ok, state, {:continue, :start_recovery}}
  end

  @impl true
  def handle_continue(:start_recovery, %State{} = t) do
    # PATCHED (fuu): return to the GenServer loop before recovery so pending
    # log/coordinator calls can be answered. Nested receive/call from this
    # callback deadlocks lock_for_recovery even when the same lock succeeds
    # from rpc.
    t = ping_all_coordinators(t)
    send(self(), :run_recovery)
    noreply(t)
  end

  @impl true
  def handle_info(:retry_stalled_recovery, %{persist_waiting: true} = t) do
    noreply(t)
  end

  def handle_info(:retry_stalled_recovery, t) do
    send(self(), :run_recovery)
    noreply(t)
  end

  @impl true
  def handle_info(:run_recovery, %{persist_waiting: true} = t) do
    noreply(t)
  end

  def handle_info(:run_recovery, t) do
    t
    |> try_to_recover()
    |> noreply()
  end

  def handle_info({:system_transaction_result, {:ok, completed}}, t) do
    # PATCHED (fuu): finish recovery after off-director system transaction.
    t =
      t
      |> Map.delete(:persist_waiting)
      |> Map.put(:state, :running)
      |> Map.put(:recovery_attempt, completed)
      |> Map.update!(:config, fn config -> Map.delete(config, :recovery_attempt) end)
      |> Map.put(:transaction_system_layout, completed.transaction_system_layout)

    t = persist_config(t)
    t = persist_new_transaction_system_layout(t)
    noreply(t)
  end

  def handle_info({:system_transaction_result, {:error, reason}}, t) do
    Logger.warning("System transaction persist failed: #{inspect(reason)}")
    Process.send_after(self(), :retry_stalled_recovery, 2_000)
    noreply(Map.delete(t, :persist_waiting))
  end

  @impl true
  def handle_info({:timeout, :ping_all_coordinators}, t) do
    t
    |> ping_all_coordinators()
    |> noreply()
  end

  @impl true
  def handle_info({:DOWN, _monitor_ref, :process, failed_pid, reason}, %{persist_waiting: true} = t) do
    # PATCHED (fuu): a commit-proxy/log death during off-director persist must
    # not take the Director down; the spawn reports {:error, reason} instead.
    Logger.warning("Component #{inspect(failed_pid)} failed during persist: #{inspect(reason)}")
    noreply(t)
  end

  def handle_info({:DOWN, _monitor_ref, :process, failed_pid, reason}, t) do
    t
    |> Map.put(:state, :stopped)
    |> stop({:shutdown, {:component_failure, failed_pid, reason}})
  end

  @impl true
  def handle_info({[:alias | ref], result}, t) when is_reference(ref) do
    Logger.warning("Director received unexpected Task reply: #{inspect(result)}")
    noreply(t)
  end

  def handle_info({:lock_result, _request_ref, _result}, t) do
    noreply(t)
  end

  def handle_info({:copy_log_result, _request_ref, _result}, t) do
    noreply(t)
  end

  @impl true

  def handle_call(:fetch_transaction_system_layout, _from, t) do
    case t.transaction_system_layout do
      nil -> reply(t, {:error, :unavailable})
      transaction_system_layout -> reply(t, {:ok, transaction_system_layout})
    end
  end

  def handle_call({:request_worker_creation, node, worker_id, kind}, _from, t) do
    t
    |> request_worker_creation(node, worker_id, kind)
    |> then(&reply(t, &1))
  end

  def handle_call({:request_to_rejoin, node, capabilities, running_services}, _from, t) do
    {:ok, updated_t} = request_to_rejoin(t, node, capabilities, Map.values(running_services), now())

    send(self(), :run_recovery)
    reply(updated_t, :ok)
  end

  @impl true

  def handle_cast({:ping, from, minimum_read_version}, t) do
    GenServer.cast(from, {:pong, {t.epoch, self()}})
    node = node(from)

    t
    |> update_last_seen_at(node, now())
    |> update_minimum_read_version(node, minimum_read_version)
    |> noreply()
  end

  def handle_cast({:pong, _from}, t), do: noreply(t)

  def handle_cast({:node_added_worker, node, worker_info}, %State{} = t) do
    t = node_added_worker(t, node, worker_info, now())
    send(self(), :run_recovery)
    noreply(t)
  end

  def handle_cast({:service_registered, service_infos}, %State{} = t) do
    t = add_services_to_directory(t, service_infos)
    send(self(), :run_recovery)
    noreply(t)
  end

  def handle_cast({:capabilities_updated, node_capabilities}, %State{} = t) do
    send(self(), :run_recovery)
    noreply(%{t | node_capabilities: node_capabilities})
  end

  # Catch-all for unexpected cast messages (e.g., from old incarnations)
  def handle_cast(message, %State{} = t) do
    Logger.debug("Director ignoring unexpected cast message: #{inspect(message)}")
    noreply(t)
  end

  @impl true
  def terminate(reason, _t) do
    Logger.error("Director terminating due to: #{inspect(reason)}")
    :ok
  end

  @spec now() :: DateTime.t()
  defp now, do: DateTime.utc_now()

  @spec get_services_from_transaction_system_layout(TransactionSystemLayout.t()) ::
          %{Worker.id() => ServiceDescriptor.t()}
  def get_services_from_transaction_system_layout(%{services: services}), do: services || %{}

  def get_services_from_transaction_system_layout(_), do: %{}

  @spec add_services_to_directory(State.t(), [{String.t(), atom(), {atom(), node()}}]) ::
          State.t()
  def add_services_to_directory(t, service_infos) do
    new_services =
      Map.new(service_infos, fn {service_id, kind, {otp_name, node}} ->
        {service_id, {kind, {otp_name, node}}}
      end)

    %{t | services: Map.merge(t.services, new_services)}
  end

  @spec try_to_recover_if_stalled(State.t()) :: State.t()
  def try_to_recover_if_stalled(%{state: :recovery} = t) do
    # If we're in recovery state, new services might resolve insufficient_nodes
    # So we should retry recovery
    try_to_recover(t)
  end

  def try_to_recover_if_stalled(t) do
    # If not in recovery state, no need to retry
    t
  end
end
