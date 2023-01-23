defmodule Ockam.Kafka.Interceptor.Protocol.Parser do
  ## Only support metadata API up to version 12

  alias Ockam.Kafka.Interceptor.Protocol.RequestHeader
  alias Ockam.Kafka.Interceptor.Protocol.ResponseHeader

  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Request, as: MetadataRequest
  alias Ockam.Kafka.Interceptor.Protocol.Metadata.Response, as: MetadataResponse

  require Logger

  @api_metadata 3

  def supported_api?(@api_metadata, api_version) when api_version <= 12 do
    true
  end

  def supported_api?(_api_key, _api_version) do
    false
  end

  def request_header_version(api_key, api_version) do
    case {api_key, api_version} do
      {@api_metadata, low} when low < 9 ->
        {:ok, 1}

      {@api_metadata, high} when high >= 9 and high <= 12 ->
        {:ok, 2}

      {_other, _version} ->
        {:error, {:unsupported_api, {api_key, api_version}}}
    end
  end

  def response_header_version(api_key, api_version) do
    case {api_key, api_version} do
      {@api_metadata, low} when low < 9 ->
        {:ok, 0}

      {@api_metadata, high} when high >= 9 and high <= 12 ->
        {:ok, 1}

      {_other, _version} ->
        {:error, {:unsupported_api, {api_key, api_version}}}
    end
  end

  ## FIXME: what should we do for api versions higher than supported??
  def parse_request_data(@api_metadata, api_version, data) do
    MetadataRequest.Parser.parse(api_version, data)
  end

  def parse_request_data(other, api_version, _data) do
    {:error, {:unsupported_api, {other, api_version}}}
  end

  def parse_response_data(@api_metadata, api_version, data) do
    MetadataResponse.Parser.parse(api_version, data)
  end

  def parse_response_data(other, api_version, _data) do
    {:error, {:unsupported_api, {other, api_version}}}
  end


  def parse_response_correlation_id(data) do
    parse_type(:int32, data)
  end

  def parse_request_header(
        header_version,
        <<api_key::signed-big-integer-size(16), api_version::signed-big-integer-size(16),
          correlation_id::signed-big-integer-size(32), rest::binary>>
      ) do
    header = %RequestHeader{
      api_key: api_key,
      api_version: api_version,
      correlation_id: correlation_id
    }

    case header_version do
      0 ->
        {:ok, %{header | header_version: 0}, rest}

      1 ->
        with {:ok, client_id, rest} <- parse_type(:nullable_string, rest) do
          {:ok, %{header | client_id: client_id, header_version: 1}, rest}
        end

      2 ->
        with {:ok, client_id, rest} <- parse_type(:nullable_string, rest),
             {:ok, tags, rest} <- parse_type(:tagged_fields, rest) do
          {:ok, %{header | client_id: client_id, tagged_fields: tags, header_version: 2}, rest}
        end

      other ->
        {:error, {:request_header_error, :unsupported_header_version, other}}
    end
  end

  def parse_request_header(_header_version, data) do
    {:error, {:request_header_error, :cannot_parse_base_header, data}}
  end

  def parse_response_header(
        0,
        request_header,
        <<_correlation_id::signed-big-integer-size(32), rest::binary>>
      ) do
    response_header = ResponseHeader.from_request_header(request_header)
    {:ok, %{response_header | header_version: 0}, rest}
  end

  def parse_response_header(
        1,
        request_header,
        <<_correlation_id::signed-big-integer-size(32), rest::binary>>
      ) do
    response_header = ResponseHeader.from_request_header(request_header)

    with {:ok, tagged_fields, rest} <- parse_type(:tagged_fields, rest) do
      {:ok, %{response_header | header_version: 1, tagged_fields: tagged_fields}, rest}
    end
  end

  def parse_response_header(header_version, _request_header, response) do
    {:error, {:response_header_error, :cannot_parse_response_header, header_version, response}}
  end

  @spec parse_type(atom() | (binary() -> {:ok, any(), binary()} | {:error, any()}), binary()) ::
          {:ok, any(), binary()} | {:error, any()}
  def parse_type(fun, data) when is_function(fun, 1) do
    fun.(data)
  end

  def parse_type(:boolean, data) do
    case data do
      <<0::8, rest::binary>> -> {:ok, false, rest}
      ## From protocol doc: `When reading a boolean value, any non-zero value is considered true.`
      <<_byte::8, rest::binary>> -> {:ok, true, rest}
      other -> {:error, {:parse_boolean, other}}
    end
  end

  def parse_type(:int32, <<number::signed-big-integer-size(32), rest::binary>>) do
    {:ok, number, rest}
  end

  def parse_type(:int16, <<number::signed-big-integer-size(16), rest::binary>>) do
    {:ok, number, rest}
  end

  def parse_type(:string, data) do
    case parse_type(:nullable_string, data) do
      {:ok, nil, _rest} ->
        {:error, {:parse_string, {:invalid_size, -1}, data}}

      {:ok, string, rest} ->
        {:ok, string, rest}

      {:error, {:parse_nullable_string, reason, data}} ->
        {:error, {:parse_string, reason, data}}
    end
  end

  def parse_type(:nullable_string, <<size::signed-big-integer-size(16), rest::binary>> = data) do
    case parse_nullable_string_of_size(size, rest) do
      {:ok, string, rest} -> {:ok, string, rest}
      {:error, reason} -> {:error, {:parse_nullable_string, reason, data}}
    end
  end

  def parse_type(:nullable_string, other) do
    {:error, {:parse_nullable_string, :invalid_data, other}}
  end

  def parse_type(:compact_nullable_string, data) do
    with {:ok, mod_size, rest} <- parse_type(:unsigned_varint, data),
         {:ok, string, rest} <- parse_nullable_string_of_size(mod_size - 1, rest) do
      {:ok, string, rest}
    else
      {:error, reason} ->
        {:error, {:parse_compact_nullable_string, reason, data}}
    end
  end

  def parse_type(:compact_string, data) do
    case parse_type(:compact_nullable_string, data) do
      {:ok, nil, _rest} ->
        {:error, {:parse_compact_string, {:invalid_size, -1}, data}}

      {:ok, string, rest} ->
        {:ok, string, rest}

      {:error, {:parse_compact_nullable_string, reason, data}} ->
        {:error, {:parse_compact_string, reason, data}}
    end
  end

  ## FIXME: bound varint
  def parse_type(:unsigned_varint, <<1::1, n::7, rest::binary>>) do
    case parse_type(:unsigned_varint, rest) do
      {:ok, m, rest} ->
        {:ok, Bitwise.bsl(m, 7) + n, rest}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def parse_type(:unsigned_varint, <<n, rest::binary>>) do
    {:ok, n, rest}
  end

  def parse_type(:unsigned_varint, other) do
    {:error, {:parse_unsigned_varint, other}}
  end

  def parse_type({:array, type}, data) do
    case parse_type({:nullable_array, type}, data) do
      {:ok, nil, _rest} ->
        {:error, {:parse_array, {:negative_size, -1, data}}}

      {:ok, array, rest} ->
        {:ok, array, rest}

      {:error, {:parse_nullable_array, reason, data}} ->
        {:parse_array, reason, data}
    end
  end

  def parse_type({:nullable_array, type}, data) do
    case data do
      <<size::signed-big-integer-size(32), rest::binary>> ->
        case parse_nullable_array_of_size(size, type, rest) do
          {:error, reason} ->
            {:error, {:parse_nullable_array, reason, data}}

          other ->
            other
        end

      other ->
        {:error, {:parse_nullable_array, :invalid_size, other}}
    end
  end

  def parse_type({:compact_array, type}, data) do
    with {:ok, mod_size, rest} <- parse_type(:unsigned_varint, data),
         {:ok, array, rest} <- parse_nullable_array_of_size(mod_size - 1, type, rest) do
      {:ok, array, rest}
    else
      {:error, reason} ->
        {:error, {:parse_compact_array, reason, data}}
    end
  end

  def parse_type(:tagged_fields, data) do
    with {:ok, tags_count, rest} <- parse_type(:unsigned_varint, data),
         {:ok, fields, rest} <- parse_fields(tags_count, rest) do
      {:ok, fields, rest}
    else
      {:error, reason} ->
        {:error, {:parse_tagged_fields, reason}}
    end
  end

  ## UUID is passed as binary
  def parse_type(:uuid, <<uuid::binary-size(16), rest::binary>>) do
    {:ok, uuid, rest}
  end

  def parse_type(:uuid, other) do
    {:error, {:parse_uuid, :invalid_data, other}}
  end

  def parse_type(other, data) do
    {:error, {:invalid_type, other, data}}
  end

  @spec parse_fields(integer(), binary(), integer(), map()) ::
          {:ok, fields :: map(), rest :: binary()} | {:error, reason :: any()}
  defp parse_fields(fields_count, data, prev \\ -1, acc \\ %{})

  defp parse_fields(0, data, _prev, acc) do
    {:ok, acc, data}
  end

  defp parse_fields(fields_count, data, prev, acc) do
    with {:ok, {tag, field}, rest} <- parse_field(data, prev) do
      parse_fields(fields_count - 1, rest, tag, Map.put(acc, tag, field))
    else
      {:error, reason} ->
        {:error, {:parse_field, reason, data, prev, acc}}
    end
  end

  defp parse_field(data, prev) do
    with {:ok, tag, rest} <- parse_type(:unsigned_varint, data),
         {:ok, size, rest} <- parse_type(:unsigned_varint, rest) do
      case tag > prev do
        true ->
          case rest do
            <<field::binary-size(size), rest::binary>> ->
              {:ok, {tag, field}, rest}

            _other ->
              {:error, {:tag_field_invalid, tag, size, rest}}
          end

        false ->
          {:error, {:tag_order_invalid, tag, prev, data}}
      end
    end
  end

  defp parse_nullable_string_of_size(size, data) do
    case size do
      -1 ->
        {:ok, nil, data}

      positive when positive >= 0 ->
        case data do
          <<string::binary-size(size), rest::binary>> ->
            {:ok, string, rest}

          other ->
            {:error, {:invalid_data, size, other}}
        end

      other ->
        {:error, {:invalid_size, other}}
    end
  end

  defp parse_nullable_array_of_size(size, type, data) do
    case size do
      negative when negative < -1 ->
        {:error, {:negative_size, size}}

      -1 ->
        {:ok, nil, data}

      positive when positive >= 0 ->
        parse_array_elements(size, type, data)
    end
  end

  defp parse_array_elements(size, type, data, acc \\ [])

  defp parse_array_elements(0, _type, data, acc) do
    {:ok, acc, data}
  end

  defp parse_array_elements(size, type, data, acc) do
    with {:ok, element, rest} <- parse_type(type, data) do
      parse_array_elements(size - 1, type, rest, acc ++ [element])
    end
  end
end
