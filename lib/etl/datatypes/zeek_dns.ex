defmodule Etl.Datatype.ZeekDns do
  @moduledoc """
  Extract information from a Zeek DNS log entry.

  This datatype module makes some alterations to the DNS log format for ingest.

  First, certain fields typically used to cross reference with other logs like
  `conn.log` are currently omitted. Rationale being, the current data sources
  of interest do not come with other data types to correlate against.

  Second, DNS log entries have an `answers` and `TTLs` field both specified as
  array types. We specifically flatten these and produce rows with single
  (query - answer - ttl) triples for ease of use with Presto.

  Lastly, we add a custom source column to track what raw data source a record
  came from .

  ---

  For documentation, see:

  https://docs.zeek.org/en/current/scripts/base/protocols/dns/main.zeek.html#type-DNS::Info
  """
  use(
    Etl.Datatype,
    columns: [
      {"id_orig_h", "VARCHAR"},
      {"id_orig_p", "BIGSIZE"},
      {"id_resp_h", "VARCHAR"},
      {"id_resp_p", "BIGSIZE"},
      {"ts", "TIMESTAMP_μs"},
      # {"uid", "VARCHAR"},
      # {"id", "VARCHAR"},
      {"proto", "VARCHAR"},
      # {"trans_id", "VARCHAR"},
      {"rtt", "DOUBLE"},
      {"query", "VARCHAR"},
      {"qclass", "BIGSIZE"},
      {"qclass_name", "VARCHAR"},
      {"qtype", "BIGSIZE"},
      {"qtype_name", "VARCHAR"},
      {"rcode", "BIGSIZE"},
      {"rcode_name", "VARCHAR"},
      {"AA", "BOOLEAN"},
      {"TC", "BOOLEAN"},
      {"RD", "BOOLEAN"},
      {"RA", "BOOLEAN"},
      {"Z", "BIGSIZE"},
      # DEV: Slight distinction from how Zeek DNS typically is formatted.
      #      The next two fields are made singular.
      {"answer", "VARCHAR"},
      {"TTL", "BIGSIZE"},
      {"rejected", "BOOLEAN"}
    ],
    partitions: [{"source", "VARCHAR"}],
    version: 1
  )

  @doc """
  ## Examples

  ### Flatten a single row with multiple answers and TTLs to multiple rows.

  iex> Etl.Datatype.ZeekDns.to_rows(%{"answers" => ["a", "b", "c"], "TTLs" => [1, 2]})
  [%{"answer" => "a", "ttl" => 1}, %{"answer" => "b", "ttl" => 2}, %{"answer" => "c", "ttl" => -1}]
  """

  def to_rows(data = %{"answers" => answers, "TTLs" => ttls}) do
    # Flatten the answers and ttls.
    base =
      data
      |> Map.drop(["answers", "TTLs"])

    # Assume that there might be more answers than TTLs.
    #
    # If there are more TTLs than answers, we can safely drop the extraneous
    # TTL values.
    padded_ttls =
      ttls
      |> Stream.concat(Stream.cycle([-1]))

    Enum.zip(answers, padded_ttls)
    |> Enum.flat_map(fn {a, t} ->
      base
      |> Map.put("answer", a)
      |> Map.put("ttl", t)
      |> to_rows()
    end)
  end

  def to_rows(data) when is_map(data) do
    data
    |> Enum.map(fn {k, v} ->
      case k do
        "id." <> k -> {"id_" <> k, v}
        _ -> {k, v}
      end
    end)
    |> Map.new()
    |> List.wrap()
  end

  def to_rows(_), do: []
end
