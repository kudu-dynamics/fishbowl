defmodule Etl.Target.StageTwo do
  @moduledoc """
  This stage transforms each decoded row in the incoming flow.

  A target can have `N` datatypes. This stage tries to produce all datatype rows
  in a single pass over the data.

  Returns a flow: [<{datatype: String.t(), row: map()}>, ...]
  """

  @doc """
  Given a flow of file blobs or lines, apply each of the available datatype
  transforms against the input data and yield the transformed row entries with a
  distinguishing datatype key.
  """

  @spec target_transform(Flow.t(), module(), map()) :: Flow.t()

  def target_transform(flow, module, metadata) do
    flow
    |> Flow.flat_map(fn data ->
      module.get_datatypes()
      |> Stream.flat_map(fn datatype ->
        data
        |> datatype.to_rows()
        # Insert common top-level fields.
        |> Enum.flat_map(&module.to_rows(metadata, data, &1))
        # Attempt to sensibly coerce types.
        |> Enum.map(&Etl.Table.process_map(datatype.get_table(), &1))
        # Add a datatype key to partition the rows.
        |> Enum.map(&{datatype.get_datatype(), &1})
      end)
    end)
  end
end
