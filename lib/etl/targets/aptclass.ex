defmodule Etl.Target.Aptclass do
  @moduledoc false

  @behaviour Etl.Target

  def get_datatypes,
    do: [Etl.Datatype.Aptclass]

  def get_rawoptions, do: [csv_separator: "|"]

  def get_rawtype, do: :csv

  def infer_metadata_from_path(_path) do
    %{}
  end

  def to_rows(metadata, _data, row) do
    row
    |> Map.merge(metadata)
    |> List.wrap()
  end
end
