defmodule Ockam.Kafka.Interceptor.Protocol.Formatter do
  @doc """
  Structured format function.
  Accepts a list of tuples {type, data} and formats a binary
  appending each element formatted with format_type

  Could be used recursively with function type like:
  `format_typed_list([{fn(value) -> format_typed_list(...) end}, ...])`
  """
  @spec format_typed_list([{atom() | (any() -> {:ok, binary()} | {:error, any()}), any()}]) ::
          {:ok, binary()} | {:error, any()}
  def format_typed_list(list) when is_list(list) do
    Enum.reduce(list, {:ok, <<>>}, fn
      {type, data}, {:ok, acc} ->
        with {:ok, bin} <- format_type(type, data) do
          {:ok, acc <> bin}
        end

      binary, {:ok, acc} when is_binary(binary) ->
        acc <> binary

      _element, {:error, reason} ->
        {:error, reason}
    end)
  end

  @spec format_type(atom() | (any() -> {:ok, binary()} | {:error, any()}), any()) ::
          {:ok, binary()} | {:error, reason :: any()}
  def format_type(fun, data) when is_function(fun, 1) do
    fun.(data)
  end

  def format_type(:boolean, value) when is_boolean(value) do
    case value do
      false -> {:ok, <<0>>}
      true -> {:ok, <<1>>}
    end
  end

  def format_type(:int32, integer) when is_integer(integer) and integer < 2_147_483_648 do
    {:ok, <<integer::signed-big-integer-size(32)>>}
  end

  def format_type(:int16, integer) when is_integer(integer) and integer < 32768 do
    {:ok, <<integer::signed-big-integer-size(16)>>}
  end

  def format_type(:string, nil) do
    {:error, :string_cannot_be_nil}
  end

  def format_type(:string, value) do
    format_type(:nullable_string, value)
  end

  def format_type(:nullable_string, nil) do
    {:ok, <<-1::signed-big-integer-size(16)>>}
  end

  def format_type(:nullable_string, value) when is_binary(value) do
    size = byte_size(value)
    {:ok, <<size::signed-big-integer-size(16), value::binary>>}
  end

  def format_type(:compact_nullable_string, nil) do
    format_type(:unsigned_varint, 0)
  end

  def format_type(:compact_nullable_string, value) when is_binary(value) do
    format_type(:compact_string, value)
  end

  def format_type(:compact_string, nil) do
    {:error, :compact_string_cannot_be_nil}
  end

  def format_type(:compact_string, value) when is_binary(value) do
    size = byte_size(value)

    format_typed_list([
      {:unsigned_varint, size + 1},
      value
    ])
  end

  def format_type(:unsigned_varint, value) when is_integer(value) and value < 4_294_967_296 do
    {:ok, format_unsigned_varint(value)}
  end

  def format_type(:unsigned_varint, value) when is_integer(value) do
    {:error, :unsigned_varint_out_of_bounds}
  end

  def format_type({:nullable_array, _type}, nil) do
    {:ok, <<-1::signed-big-integer-size(32)>>}
  end

  def format_type({:nullable_array, type}, array) when is_list(array) do
    format_type({:array, type}, array)
  end

  def format_type({:array, type}, array) when is_list(array) do
    size = length(array)

    with {:ok, elements_bin} <- format_array_elements(type, array) do
      {:ok, <<size::signed-big-integer-size(32), elements_bin::binary>>}
    end
  end

  def format_type({:compact_array, _type}, nil) do
    format_type(:unsigned_varint, 0)
  end

  def format_type({:compact_array, type}, array) when is_list(array) do
    size = length(array)

    format_typed_list([
      {:unsigned_varint, size + 1},
      {fn array -> format_array_elements(type, array) end, array}
    ])
  end

  def format_type(:tagged_fields, fields_map) when is_map(fields_map) do
    size = map_size(fields_map)

    format_typed_list([
      {:unsigned_varint, size},
      {&format_fields/1, fields_map}
    ])
  end

  def format_type(:tagged_field, {tag, value}) when is_integer(tag) and is_binary(value) do
    size = byte_size(value)

    format_typed_list([
      {:unsigned_varint, tag},
      {:unsigned_varint, size},
      value
    ])
  end

  ## UUID is passed as binary
  def format_type(:uuid, value) when is_binary(value) do
    {:ok, value}
  end

  def format_type(other, data) do
    {:error, {:invalid_type, other, data}}
  end

  def format_array_elements(type, array) when is_list(array) do
    array
    |> Enum.map(fn element -> {type, element} end)
    |> format_typed_list()
  end

  def format_fields(fields_map) when is_map(fields_map) do
    fields_map
    |> Enum.sort_by(fn {tag, _value} -> tag end)
    |> Enum.map(fn element -> {:tagged_field, element} end)
    |> format_typed_list()
  end

  def format_unsigned_varint(value) when value < 128 do
    <<value>>
  end

  def format_unsigned_varint(value) do
    <<1::1, value::7>> <> format_unsigned_varint(Bitwise.bsr(value, 7))
  end
end
