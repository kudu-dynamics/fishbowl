defmodule Etl.Datatype.VirustotalScan do
  @moduledoc """
  AV scanner data extracted from VirusTotal scan reports.
  """
  alias Etl.Datatype.Virustotal

  use(
    Etl.Datatype,
    columns: [
      {"file_sha256", "VARCHAR(64)"},
      {"scanner_detected", "BOOLEAN"},
      {"scanner_name", "VARCHAR"},
      {"scanner_result", "VARCHAR"},
      {"scanner_version", "VARCHAR"},
      {"scans_positive", "BIGSIZE"},
      {"scans_total", "BIGSIZE"},
      {"timestamp", "BIGSIZE"}
    ],
    partitions: [
      {"year", "VARCHAR(4)"},
      {"month", "VARCHAR(2)"}
    ],
    partition_with_date: false,
    version: 1
  )

  def to_rows(%{
        "file_sha256" => file_sha256,
        "positives" => positives,
        "scans" => scans,
        "timestamp" => timestamp,
        "total" => total
      })
      when is_map(scans) and map_size(scans) > 0 do
    scans
    |> Map.to_list()
    |> Enum.map(fn {name, scan} ->
      [
        {"file_sha256", file_sha256},
        {"scanner_detected", Map.get(scan, "detected")},
        {"scanner_name", name},
        {"scanner_result", Map.get(scan, "result")},
        {"scanner_version", Map.get(scan, "version")},
        {"scans_positive", positives},
        {"scans_total", total},
        {"timestamp", timestamp}
      ]
      |> Map.new()
    end)
  end

  def to_rows(data) when is_map(data) and map_size(data) > 0 do
    data
    |> Virustotal.parse_scan_id()
    |> to_rows()
  end

  def to_rows(_), do: []
end
