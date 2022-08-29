defmodule Etl.Target.StageFive do
  @moduledoc """
  This stage takes pairs of file paths with encoded partition information and
  datatype modules and writes the appropriate tables to Trino.
  """

  @spec write_tables(Flow.t(), keyword()) :: Flow.t()

  def write_tables(flow, opts) do
    upload? = Keyword.get(opts, :upload?)

    flow
    # Update only a single partition at a time.
    |> Flow.partition(stages: 1)
    |> Flow.map(fn {_, module} ->
        module_table = module.get_table()
        IO.puts("Name: Table: #{inspect(module_table)}")
        table_name = Keyword.get(opts, :table_name, nil)
        table_name = if table_name do
                    IO.puts("Name 1: #{table_name}")
                    Keyword.get(opts, :table_name)
                else
                    IO.puts("Name 2: #{Map.get(module_table, :name)}")
                    Map.get(module_table, :name)
                end
        module_table = %{module_table | name: table_name}
        IO.puts("Name: #{table_name} Opts: #{inspect(opts)} Table: #{inspect(module_table)}")
      if upload? do
        #module.get_table()
        module_table 
        |> Etl.Table.write_table(opts)
      end

      :ok
    end)
  end
end
