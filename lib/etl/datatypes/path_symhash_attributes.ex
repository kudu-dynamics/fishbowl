defmodule Etl.Datatype.PathSymhashAttributes do
  @moduledoc """
  AV scanner data extracted from VirusTotal scan reports.
  """

  use(
    Etl.Datatype,
    columns: [
      {"file_sha256", "VARCHAR(64)"},
      {"attribute", "VARCHAR(64)"},
      {"num_files", "BIGSIZE"},
    ],
    version: 1
  )

  def to_rows(%{
        "file_sha256" => file_sha256,
        "attribute" => attribute,
        "num_files" => num_files,
      })
      do
      [
        %{
            "file_sha256" => file_sha256,
            "attribute" => attribute,
            "num_files" => num_files,
        }
      ]

  end

  def to_rows(data) when is_map(data) and map_size(data) > 0 do
    data
    |> to_rows()
  end

  def to_rows(_), do: []
end
