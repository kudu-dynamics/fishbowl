defmodule Etl.Target.IsiGawseed do
  @moduledoc false

  @behaviour Etl.Target

  def get_datatypes,
    do: [
      Etl.Datatype.IsiDnsRegexp,
      Etl.Datatype.IsiIpAddress,
      Etl.Datatype.IsiIpAddressWithMask,
      Etl.Datatype.IsiIpAddressWithRange,
      Etl.Datatype.IsiSha256Sum,
      Etl.Datatype.IsiUrlRegexp
    ]

  def get_rawoptions, do: []

  def get_rawtype, do: :jsonlines

  def infer_metadata_from_path(_path) do
    %{}
  end

  def to_rows(metadata, _data, row) do
    row
    |> Map.merge(metadata)
    |> List.wrap()
  end
end
