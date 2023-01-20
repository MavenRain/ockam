defmodule Ockam.Kafka.Interceptor.Handler do
  @moduledoc """
  Functions to handle successfully parsed kafka messages

  Separate module from Ockam.Kafka.Interceptor for code readability
  """

  alias Ockam.Kafka.Interceptor.Protocol.RequestHeader
  alias Ockam.Kafka.Interceptor.Protocol.ResponseHeader

  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Request, as: MetadataRequest
  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Response, as: MetadataResponse
  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Response.Broker

  require Logger

  @outlet_prefix "kafka_outlet_"
  @base_port 9001

  ## FIXME: set outlet route from the options
  @outlet_route ["interceptor"]

  ## Currently matching by the response type, could be matching using api_key from RequestHeader
  def handle_kafka_request(%RequestHeader{}, %MetadataRequest{} = request, state) do
    Logger.info("Metadata request: #{inspect(request)}")
    {:ok, state}
  end

  def handle_kafka_request(%RequestHeader{api_key: api_key}, _request, state) do
    Logger.info("Ignoring request with api key #{inspect(api_key)}")
    {:ok, state}
  end

  ## Currently matching by the response type, could be matching using api_key from ResponseHeader
  def handle_kafka_response(%ResponseHeader{}, %MetadataResponse{} = response, state) do
    Logger.info("Metadata response: #{inspect(response)}")

    with :ok <- create_broker_outlets(response),
         {:ok, new_response, state} <- create_broker_inlets(response, state) do
      {:ok, new_response, state}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  def handle_kafka_response(%ResponseHeader{api_key: api_key}, _response, state) do
    Logger.info("Ignoring response with api key #{inspect(api_key)}")
    {:ok, state}
  end

  ## TODO: manage inlets and outlets from a separate module component

  def create_broker_inlets(%MetadataResponse{brokers: brokers} = response, %{inlets: inlets} = state) do
    metadata_inlet_nodes = Enum.map(brokers, fn(broker) -> broker.node_id end) |> Enum.sort()
    existing_inlet_nodes = inlets |> Enum.map(fn({node_id, _pid}) -> node_id end) |> Enum.sort()

    state = case metadata_inlet_nodes == existing_inlet_nodes do
      true ->
        state
      false ->
        update_inlets(existing_inlet_nodes, metadata_inlet_nodes, state)
    end

    brokers = Enum.map(brokers, fn(%Broker{node_id: node_id} = broker) ->
      %{broker | host: "localhost", port: inlet_port(node_id)}
    end)
    {:ok, %{response | brokers: brokers}, state}
  end

  def create_broker_outlets(%MetadataResponse{brokers: brokers}) do
    metadata_outlets =
      Enum.map(brokers, fn broker ->
        {to_string(broker.node_id), {broker.host, broker.port}}
      end)
      |> Map.new()

    existing_outlets =
      Ockam.Node.list_addresses()
      |> Enum.filter(fn address -> String.starts_with?(address, @outlet_prefix) end)
      |> Enum.map(fn address ->
        Ockam.Node.whereis(address) |> :sys.get_state() |> Map.take([:address, :worker_options])
      end)
      |> Enum.map(fn %{address: address, worker_options: options} ->
        target_host = Keyword.fetch!(options, :target_host)
        target_port = Keyword.fetch!(options, :target_port)

        {
          String.replace_prefix(address, @outlet_prefix, ""),
          {target_host, target_port}
        }
      end)
      |> Map.new()

    case metadata_outlets == existing_outlets do
      true ->
        :ok

      false ->
        update_outlets(existing_outlets, metadata_outlets)
    end
  end

  def update_inlets(existing_inlets, metadata_inlets, state) do
    extra = existing_inlets -- metadata_inlets

    state = Enum.reduce(extra, state, fn(node_id, state) ->
      stop_inlet(node_id, state)
    end)

    missing = metadata_inlets -- existing_inlets

    Enum.reduce(missing, state, fn(node_id, state) ->
      start_inlet(node_id, state)
    end)
  end

  def stop_inlet(node_id, %{inlets: inlets} = state) do
    case Map.fetch(inlets, node_id) do
      {:ok, pid} ->
        ## FIXME: manage inlets in a supervisor
        try do
          GenServer.stop(pid)
        catch
          :exit, {:noproc, _} ->
            :ok
        end
        %{state | inlets: Map.delete(inlets, node_id)}
      :error ->
        state
    end
  end

  def start_inlet(node_id, %{inlets: inlets} = state) when is_integer(node_id) do
    port = inlet_port(node_id)
    peer_route = @outlet_route ++ [outlet_address(node_id)]
    ## FIXME: handle failures
    case Ockam.Transport.Portal.InletListener.start_link(
      port: port,
      peer_route: peer_route) do
      {:ok, pid} ->
        %{state | inlets: Map.put(inlets, node_id, pid)}
      {:ok, pid, _extra} ->
        %{state | inlets: Map.put(inlets, node_id, pid)}
    end
  end

  def inlet_port(node_id) do
    @base_port + node_id
  end

  def outlet_address(node_id) do
    @outlet_prefix <> to_string(node_id)
  end

  ## TODO: maybe we want to terminate existing connections when outlets are reshuffled??
  def update_outlets(existing_outlets, metadata_outlets) do
    ## FIXME: there could be concurrent requests to metadata.
    ## We should make the outlet reconciliation synchronized in gen_server

    ## Terminate all old outlets and start new ones.
    ## TODO: we could skip outlets with same config
    Enum.each(existing_outlets, fn {node_id, _} ->
      stop_outlet(node_id)
    end)

    Enum.each(metadata_outlets, fn {node_id, {target_host, target_port}} ->
      start_outlet(node_id, target_host, target_port)
    end)

    :ok
  end

  def stop_outlet(node_id) do
    Ockam.Node.stop(@outlet_prefix <> to_string(node_id))
  end

  def start_outlet(node_id, target_host, target_port) do
    address = @outlet_prefix <> to_string(node_id)
    ## FIXME: handle failures
    Ockam.Session.Spawner.create(
      address: address,
      worker_mod: Ockam.Transport.Portal.OutletWorker,
      worker_options: [target_host: target_host, target_port: target_port]
    )
  end
end
