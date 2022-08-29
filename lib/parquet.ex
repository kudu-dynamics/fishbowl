defmodule Fishbowl.Parquet do
  @moduledoc """
  Utilities for converting data into Parquet data.
  """
  use Export.Python
  require Logger

  @doc """
  Convert an Enum of maps into a Parquet table.
  """

  @spec encode(Enumerable.t()) :: String.t()

  def encode(rows) do
    Logger.debug("Encoding parquet")

    data =
      rows
      |> Enum.to_list()
      |> Msgpax.pack!()
      |> IO.iodata_to_binary()

    {:ok, py} = Python.start(python: "python3")

    result =
      py
      |> Python.call(
        convert_to_parquet_from_msgpack(data),
        from_file: "parquet"
      )

    py |> Python.stop()

    result
  end

  @doc """
  Given a JSON lines file path, convert the contents to Parquet.
  """

  @spec encode_file(String.t()) :: String.t()

  def encode_file(path) do
    Logger.debug("Encoding parquet file")

    {:ok, py} = Python.start(python: "python3")

    result =
      py
      |> Python.call(
        convert_to_parquet_from_file(path),
        from_file: "parquet"
      )

    py |> Python.stop()

    result
  end
end
