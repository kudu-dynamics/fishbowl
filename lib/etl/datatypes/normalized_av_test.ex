defmodule Etl.Datatype.NormalizedAvTest do
  @moduledoc """
  AV scanner data extracted from VirusTotal scan reports.
  """

  use(
    Etl.Datatype,
    columns: [
      {"file_sha256", "VARCHAR(64)"},
      {"scan_id", "VARCHAR(64)"},
      {"scanner_detected", "BOOLEAN"},
      {"scans_positive", "BIGSIZE"},
      {"scans_total", "BIGSIZE"},
      {"scanner_name", "VARCHAR"},
      {"scanner_version", "VARCHAR"},
      {"scanner_result", "VARCHAR"},
      {"category", "VARCHAR"},
      {"platform", "VARCHAR"},
      {"variant", "VARCHAR"},
      {"family", "VARCHAR"}
    ],
    version: 1
  )

  def to_rows(%{
        "file_sha256" => file_sha256,
        "scan_id" => scan_id,
        "scanner_detected" => scanner_detected,
        "scans_positive" => scans_positive,
        "scans_total" => scans_total,
        "scanner_name" => scanner_name,
        "scanner_version" => scanner_version,
        "scanner_result" => scanner_result,
        "category" => category,
        "platform" => platform,
        "variant" => variant,
        "family" => family
      })
      do
      family = if family do 
            family
        else
            variant
        end
      [
        %{
          "file_sha256" => file_sha256,
          "scan_id" => scan_id,
          "scanner_detected" => scanner_detected,
          "scans_positive" => scans_positive,
          "scans_total" => scans_total,
          "scanner_name" => scanner_name,
          "scanner_version" => scanner_version,
          "scanner_result" => scanner_result,
          "category" => category,
          "platform" => platform,
          "variant" => variant,
          "family" => family
        }
      ]

  end

  def to_rows(data) when is_map(data) and map_size(data) > 0 do
    data
    |> to_rows()
  end

  def to_rows(_), do: []
end
