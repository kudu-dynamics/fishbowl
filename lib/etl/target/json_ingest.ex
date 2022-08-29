defmodule Etl.Target.JsonIngest do
  @moduledoc false

  @behaviour Etl.Target

  def get_datatypes,
    do: [
      Etl.Datatype.JsonIngest
    ]

  def get_rawoptions, do: []

  def get_rawtype, do: :json

  def infer_metadata_from_path(path) do
    [
      {"date", path |> Fishbowl.infer_date_from_path() |> Date.to_iso8601()}
    ]
    |> Map.new()
  end

  def to_rows(metadata, _data, row) do
    row
    |> Map.merge(metadata)
    |> List.wrap()
  end
end
