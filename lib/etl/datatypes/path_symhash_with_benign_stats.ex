defmodule Etl.Datatype.PathSymhashWithBenignStats do
  @moduledoc """
  AV scanner data extracted from VirusTotal scan reports.
  """
  require Logger

  use(
    Etl.Datatype,
    columns: [
      {"attribute", "VARCHAR(64)"},
      {"scanner", "VARCHAR(64)"},
      {"family", "VARCHAR(64)"},
      {"num_benign_files", "BIGSIZE"},
      {"num_files", "BIGSIZE"},
      {"num_files_with_this_family", "BIGSIZE"},
      {"num_negative_files", "BIGSIZE"},
      {"num_positive_files", "BIGSIZE"},
      {"num_scanned_files", "BIGSIZE"},
    ],
    version: 1
  )

  def to_rows(%{
        "attribute" => attribute,
        "scanner" => scanner,
        "family" => family,
        "num_benign_files" => num_benign_files,
        "num_files" => num_files,
        "num_files_with_this_family" => num_files_with_this_family,
        "num_negative_files" => num_negative_files,
        "num_positive_files" => num_positive_files,
        "num_scanned_files" => num_scanned_files,
      })
      do
      [
        %{
            "attribute" => attribute,
            "scanner" => scanner,
            "family" => family,
            "num_benign_files" => num_benign_files,
            "num_files" => num_files,
            "num_files_with_this_family" => num_files_with_this_family,
            "num_negative_files" => num_negative_files,
            "num_positive_files" => num_positive_files,
            "num_scanned_files" => num_scanned_files,
        }
      ]

  end

  def to_rows(data) when is_map(data) and map_size(data) > 0 do
    data
    |> to_rows()
  end

  def to_rows(_), do: []
end
