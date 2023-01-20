defmodule Ockam.Kafka.Interceptor.Protocol.RequestHeader do
  defstruct [:header_version, :api_key, :api_version, :correlation_id, :client_id, :tagged_fields]
end
