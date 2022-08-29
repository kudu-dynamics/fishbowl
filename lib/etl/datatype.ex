defmodule Etl.Datatype do
  @moduledoc """
  Define an ETL datatype module.

  A datatype module is responsible for ingesting a data map and performing the
  necessary filters and transforms to produce the specified table.

  This module provides itself as a macro for defining datatype modules.

  ```
  use(
    Etl.Datatype,
    columns: [
      {"foo", "VARCHAR"},
      {"baz", "TIMESTAMP"},
    ],
    datatype: "example",
    partitions: [{"bar", "BIGINT"}],
    version: 124,
  )
  ```
  """
  alias Etl.Table

  @callback get_datatype() :: String.t()
  @callback get_table() :: Table.t()
  @callback to_rows(map()) :: [map()]

  defmacro __using__(opts) do
    columns = Keyword.get(opts, :columns)

    # Derive the datatype/table name from the module name automatically unless
    # an explicit value is given.
    auto_name =
      __CALLER__.module
      |> to_string()
      |> Recase.to_snake()
      |> String.trim_leading("elixir_etl_datatype_")

    datatype = Keyword.get(opts, :datatype, auto_name)
    name = Keyword.get(opts, :table_name, auto_name)
    
    # Partitions are provided as a list of column tuples.
    #
    # [{"foo", "VARCHAR"}]
    partitions = Keyword.get(opts, :partitions, [])
    # Most tables are partitioned by date.
    #
    # This inserts a default leading `{"date", "VARCHAR(10)"}` column and
    # partition.
    partition_with_date = Keyword.get(opts, :partition_with_date, true)
    version = Keyword.get(opts, :version)

    # Dynamic modifications.
    if partition_with_date do
      partitions = [{"date", "VARCHAR(10)"}] ++ partitions
      bind(columns, datatype, name, partitions, version)
    else
      bind(columns, datatype, name, partitions, version)
    end
  end

  defp bind(columns, datatype, name, partitions, version) do
    table = %Table{
      :columns => columns ++ partitions,
      :name => name,
      :partitions => Enum.map(partitions, &elem(&1, 0)),
      :version => version
    }

    quote do
      alias Etl.Table

      @behaviour Etl.Datatype

      # A datatype is often self-descriptive from the module name.
      # Allow this macro to populate this field if a value isn't given.
      #
      # PurifierBasic => "purifier_basic"
      @datatype unquote(datatype)
      @table unquote(Macro.escape(table))

      @spec get_datatype() :: String.t()
      def get_datatype, do: @datatype

      @spec get_table() :: Table.t()
      def get_table, do: @table

      @spec to_rows(map()) :: [map()]
    end
  end
end
