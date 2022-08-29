defmodule Etl.Target.Virustotal do
  @moduledoc false

  @behaviour Etl.Target

  def get_datatypes,
    do: [
      Etl.Datatype.VirustotalScan,
      Etl.Datatype.VirustotalSubmission
    ]

  def get_rawoptions, do: []

  def get_rawtype, do: :jsonlines

  @doc """
  ### Examples

      iex> Etl.Target.Virustotal.infer_metadata_from_path("virustotal_files/3/2017_04/vtfeed_2017_04_0.jsonl.gz")
      ...> %{"month" => "04", "year" => "2017"}

      iex> Etl.Target.Virustotal.infer_metadata_from_path("virustotal_files/3/2017_04/vtfeed_2017_04_0.jsonl.gz")
      ...> %{"month" => "04", "year" => "2017"}
  """

  def infer_metadata_from_path(path) do
    date =
      Fishbowl.infer_date_from_path(
        path,
        [
          "(?<year>\\d{4})",
          "(?<month>\\d{2})"
        ]
      )

    pad_fn = fn x, y -> x |> Integer.to_string() |> String.pad_leading(y, "0") end

    case date do
      nil ->
        []

      _ ->
        [
          {"year", pad_fn.(date.year, 4)},
          {"month", pad_fn.(date.month, 2)}
        ]
        |> Map.new()
    end
  end

  def to_rows(metadata, _data, row) do
    row
    |> Map.merge(metadata)
    |> List.wrap()
  end
end
