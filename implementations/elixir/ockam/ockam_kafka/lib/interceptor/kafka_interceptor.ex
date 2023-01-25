defmodule Ockam.Kafka.Interceptor do
  @moduledoc """
  Implementation of Ockam.Transport.Portal.Interceptor callbacks
  to intercept kafka protocol messages.

  Supports pluggable handlers for requests and responses.

  Used with `Ockam.Kafka.Interceptor.MetadataHandler` to intercept metadata messages

  Options:
  :request_handlers - handler functions for requests (see `Ockam.Kafka.Interceptor.MetadataHandler`)
  :response_handlers - handler functions for responses
  :handler_options - additional options to be used by handlers
  """

  @behaviour Ockam.Transport.Portal.Interceptor

  alias Ockam.Kafka.Interceptor.Protocol.Parser

  alias Ockam.Kafka.Interceptor.Protocol.RequestHeader

  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Response, as: MetadataResponse

  require Logger

  @impl true
  def setup(options, state) do
    handler_options = Keyword.get(options, :handler_options, [])
    request_handlers = Keyword.get(options, :request_handlers, [])
    response_handlers = Keyword.get(options, :response_handlers, [])
    correlations = %{}

    {:ok,
     Map.put(state, :correlations, correlations)
     |> Map.put(:request_handlers, request_handlers)
     |> Map.put(:response_handlers, response_handlers)
     |> Map.put(:handler_options, handler_options)}
  end

  ## Kafka requests
  @impl true
  def handle_outer_payload(tunnel_payload, state) do
    process_kafka_payload(:request, tunnel_payload, state)
  end

  ## Kafka responses
  @impl true
  def handle_inner_payload(tunnel_payload, state) do
    process_kafka_payload(:response, tunnel_payload, state)
  end

  @impl true
  def handle_outer_signal(_signal, state) do
    {:ok, state}
  end

  @impl true
  def handle_inner_signal(_signal, state) do
    {:ok, state}
  end

  def process_kafka_payload(type, data, state) do
    case do_process_kafka_payload(type, data, state) do
      {:ok, new_data, new_state} ->
        Logger.info("
          Old data: #{inspect(data, limit: :infinity, printable_limit: :infinity)}
          New data: #{inspect(new_data, limit: :infinity, printable_limit: :infinity)}")
        {:ok, new_data, new_state}

      {:error, {:unsupported_api, _other}} ->
        {:ok, state}

      {:error, reason} ->
        Logger.warn(
          "Kafka interceptor processing error for type: #{inspect(type)} : #{inspect(reason)}"
        )

        ## Tunnel messages should still be forwarded even if processing failed
        {:ok, state}
    end
  end

  ## FIXME: handle batched messages
  defp do_process_kafka_payload(type, data, state) do
    processed =
      case data do
        <<size::signed-big-integer-size(32), message::binary-size(size)>> ->
          case type do
            :request -> process_kafka_request(message, state)
            :response -> process_kafka_response(message, state)
          end

        _other ->
          {:error, {:invalid_kafka_message_format, type, data}}
      end

    with {:ok, message, state} <- processed do
      size = byte_size(message)

      {:ok, <<size::signed-big-integer-size(32), message::binary>>, state}
    end
  end

  ## FIXME: change return types so error results can also update state (correlation id tracking)
  @spec process_kafka_response(binary(), state :: map()) ::
          {:ok, binary(), state :: map()} | {:error, reason :: any()}
  def process_kafka_response(
        response,
        state
      ) do
    case Parser.parse_response_correlation_id(response) do
      {:ok, correlation_id, _rest} ->
        case get_request_header(correlation_id, state) do
          {:ok, request_header} ->
            state = cleanup_request_header(correlation_id, state)
            ## FIXME: change layers here to always return state from this point
            ## this is required to cleanup correlation ids
            process_kafka_response(request_header, response, state)

          {:error, :not_found} ->
            ## FIXME: do we need to break the connection here?
            {:error, {:correlation_id_not_found, correlation_id, response}}

          {:error, :not_tracked} ->
            {:ok, response, state}
        end

      {:error, _reason} ->
        {:error, {:response_header_error, :cannot_parse_correlation_id, response}}
    end
  end

  ## FIXME: can we refactor that??
  ## FIXME: different error modes for tolerated and non-tolerated errors
  ## make it break the connection on some errors??
  @spec process_kafka_response(RequestHeader.t(), binary(), state :: map()) ::
          {:ok, binary(), state :: map()} | {:error, reason :: any()}
  def process_kafka_response(
        %RequestHeader{api_key: api_key, api_version: api_version} = request_header,
        response,
        state
      ) do
    with {:ok, response_header_version} <- Parser.response_header_version(api_key, api_version),
         {:ok, response_header, response_data} <-
           Parser.parse_response_header(response_header_version, request_header, response),
         {:ok, response_content, <<>>} <-
           Parser.parse_response_data(api_key, api_version, response_data) do
      case handle_kafka_response(response_header, response_content, state) do
        {:ok, state} ->
          {:ok, response, state}

        {:ok, updated_response_content, state} ->
          ## Simplify reconstruction of response header by using the original header binary
          ## since we don't change the header we can just reuse it with new response content
          old_header_binary_size = byte_size(response) - byte_size(response_data)
          <<old_header::binary-size(old_header_binary_size), ^response_data::binary>> = response

          case reconstruct_response(old_header, updated_response_content) do
            {:ok, new_response} -> {:ok, new_response, state}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp handle_kafka_response(
         response_header,
         response_content,
         %{response_handlers: handlers} = state
       ) do
    Enum.reduce(handlers, {:ok, state}, fn
      handler, {:ok, state} -> handler.(response_header, response_content, state)
      handler, {:ok, prev_response, state} -> handler.(response_header, prev_response, state)
      _handler, {:error, reason} -> {:error, reason}
    end)
  end

  @spec process_kafka_request(binary(), state :: map()) ::
          {:ok, binary(), state :: map()} | {:error, reason :: any()}
  def process_kafka_request(
        request,
        state
      ) do
    ## FIXME: handle unsupported_api error from here and record correlation_id as not_tracked
    ## Can use Parser.supported_api? function for that
    ## Request header is always prefixed with header version 0
    with {:ok, %RequestHeader{api_key: api_key, api_version: api_version}, _rest} <-
           Parser.parse_request_header(0, request),
         {:ok, request_header_version} <- Parser.request_header_version(api_key, api_version),
         ## We might want to interrupt the connection if we can't parse the header
         ## TODO: different error mode
         {:ok, request_header, request_data} <-
           Parser.parse_request_header(request_header_version, request),
         state <- save_request_header(request_header, state),
         ## FIXME: request content parsing should be a separate layer?
         :ok <-
           Logger.info(
             "Request header: #{inspect(request_header)}, request data: #{inspect(request_data)}"
           ),
         {:ok, request_content, <<>>} <-
           Parser.parse_request_data(api_key, api_version, request_data) do
      case handle_kafka_request(request_header, request_content, state) do
        {:ok, state} ->
          {:ok, request, state}

        {:ok, updated_request_content, state} ->
          ## Simplify reconstruction of request header by using the original header binary
          ## since we don't change the header we can just reuse it with new request content
          old_header_binary_size = byte_size(request) - byte_size(request_data)
          <<old_header::binary-size(old_header_binary_size), ^request_data::binary>> = request

          case reconstruct_request(old_header, updated_request_content) do
            {:ok, new_request} -> {:ok, new_request, state}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp handle_kafka_request(
         request_header,
         request_content,
         %{request_handlers: handlers} = state
       ) do
    Enum.reduce(handlers, {:ok, state}, fn
      handler, {:ok, state} -> handler.(request_header, request_content, state)
      handler, {:ok, prev_request, state} -> handler.(request_header, prev_request, state)
      _handler, {:error, reason} -> {:error, reason}
    end)
  end

  ## Currently we don't modify requests, hence we don't support reconstructing form struct
  @spec reconstruct_request(binary(), request :: any()) ::
          {:ok, binary()} | {:error, reason :: any()}
  def reconstruct_request(_header_binary, request) do
    {:error, {:unsupported_request, request}}
  end

  @spec reconstruct_response(binary(), response :: any()) ::
          {:ok, binary()} | {:error, reason :: any()}
  def reconstruct_response(header_binary, %MetadataResponse{} = response) do
    case MetadataResponse.Formatter.format(response) do
      {:ok, response_binary} ->
        {:ok, header_binary <> response_binary}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def reconstruct_response(_header_binary, response) do
    {:error, {:unsupported_response, response}}
  end

  ## Correlation ID tracking

  def save_request_header(
        %RequestHeader{correlation_id: correlation_id} = header,
        %{correlations: correlations} = state
      ) do
    Map.put(state, :correlations, Map.put(correlations, correlation_id, header))
  end

  def get_request_header(correlation_id, %{correlations: correlations}) do
    case Map.fetch(correlations, correlation_id) do
      {:ok, header} -> {:ok, header}
      ## FIXME: distinguish between not_found and not_tracked??
      :error -> {:error, :not_found}
    end
  end

  def cleanup_request_header(correlation_id, %{correlations: correlations} = state) do
    Map.put(state, :correlations, Map.delete(correlations, correlation_id))
  end
end
