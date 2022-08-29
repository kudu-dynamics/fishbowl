defmodule Etl.Table do
  @moduledoc """
  Define a common interface for working with Presto tables.
  """
  alias Clients.Presto
  require Logger
  use TypedStruct

  # XXX: make these configurable by environment variable (12factor)
  # XXX replace with schema and alluxio location
  @catalog "alluxio"
  @schema "<schema>"
  @external_location "alluxio://<alluxio_location>"

  typedstruct enforce: true do
    field(:catalog, String.t(), default: @catalog)
    # DEV: Columns cannot have special characters aside from '_'.
    field(:columns, nonempty_list({String.t(), String.t()}))
    field(:external_location, String.t(), default: @external_location)
    field(:name, String.t())
    field(:partitions, list(String.t()), default: [])
    field(:schema, String.t(), default: @schema)
    field(:version, non_neg_integer())
  end

  @doc """
  Get the table's name, prepending an environment namespace.
  """
  @spec table_name(Etl.Table.t()) :: String.t()
  def table_name(table) do
    table.name
    |> Fishbowl.env_prepend()
  end

  @doc """
  Format the table's location to be used in queries.

  ## Examples

      iex> %Etl.Table{:catalog => "a",
      ...>            :name => "c",
      ...>            :schema => "b",
      ...>            :version => 1,
      ...>            :columns => [{"foo", "VARCHAR"}]}
      ...> |> Etl.Table.format_location
      "a.b.dev_c"
  """
  @spec format_location(Etl.Table.t()) :: String.t()
  def format_location(table) do
    Enum.join([table.catalog, table.schema, table_name(table)], ".")
  end

  @doc """
  Format the table's location to be used in queries.

  ## Examples

      iex> %Etl.Table{:catalog => "a",
      ...>            :external_location => "alluxio://localhost:9998",
      ...>            :name => "c",
      ...>            :schema => "b",
      ...>            :version => 1,
      ...>            :columns => [{"foo", "VARCHAR"}]}
      ...> |> Etl.Table.format_external_location
      "alluxio://localhost:9998/dev_c/1"
  """
  @spec format_external_location(Etl.Table.t()) :: String.t()
  def format_external_location(table) do
    Enum.join([table.external_location, format_storage_path(table)], "/")
  end

  @spec format_storage_path(Etl.Table.t()) :: String.t()
  def format_storage_path(table) do
    Enum.join([table_name(table), table.version], "/")
  end

  @doc """
  For a given table, produce the corresponding SQL drop table statement.

  ## Examples

      iex> %Etl.Table{:catalog => "a",
      ...>            :name => "c",
      ...>            :schema => "b",
      ...>            :version => 1,
      ...>            :columns => [{"foo", "VARCHAR"}, {"bar", "DOUBLE"}]}
      ...> |> Etl.Table.format_columns
      "\\\"foo\\\" VARCHAR,\\n\\\"bar\\\" DOUBLE"
  """
  @spec format_columns(Etl.Table.t()) :: String.t()
  def format_columns(table) do
    table.columns
    # Translate custom column types back to real column types.
    |> Enum.map(fn pair ->
      case pair do
        {field_name, "BIGSIZE"} -> {field_name, "BIGINT"}
        {field_name, "TIMESTAMP_" <> _} -> {field_name, "TIMESTAMP"}
        _ -> pair
      end
    end)
    |> Enum.map(fn {field_name, field_type} -> "\"#{field_name}\" #{field_type}" end)
    |> Enum.join(",\n")
  end

  @doc """
  For a given table, produce the corresponding SQL with block for use in
  creating a table.

  ## Examples

      iex> %Etl.Table{:catalog => "a",
      ...>            :columns => [{"foo", "VARCHAR"}, {"bar", "DATE"}],
      ...>            :external_location => "s3",
      ...>            :name => "table",
      ...>            :schema => "b",
      ...>            :version => 1,
      ...>            :partitions => ["date"]}
      ...> |> Etl.Table.format_sql_with
      "external_location = 's3/dev_table/1',\\nformat = 'PARQUET',\\npartitioned_by = ARRAY['date']"
  """
  @spec format_sql_with(Etl.Table.t()) :: String.t()
  def format_sql_with(table = %{:partitions => []}) do
    format_sql_with_(table, "")
  end

  def format_sql_with(table) do
    partition_str =
      table.partitions
      |> Enum.map(fn s -> "'#{s}'" end)
      |> Enum.join(", ")

    format_sql_with_(table, "partitioned_by = ARRAY[#{partition_str}]")
  end

  defp format_sql_with_(table, partition_str) do
    [
      "external_location = '#{format_external_location(table)}'",
      "format = 'PARQUET'",
      partition_str
    ]
    |> Enum.reject(fn s -> s == nil || String.length(s) == 0 end)
    |> Enum.join(",\n")
  end

  @doc """
  For a given table, produce the corresponding SQL drop table statement.

  ## Examples

      iex> %Etl.Table{:catalog => "a",
      ...>            :name => "c",
      ...>            :schema => "b",
      ...>            :version => 1,
      ...>            :columns => {"foo", "VARCHAR"}}
      ...> |> Etl.Table.format_sql_drop_table
      "DROP TABLE IF EXISTS a.b.dev_c"
  """
  @spec format_sql_drop_table(Etl.Table.t()) :: String.t()
  def format_sql_drop_table(table) do
    ~s{DROP TABLE IF EXISTS #{format_location(table)}}
  end

  @doc """
  For a given table, produce the corresponding SQL create table statement.

  Notably, this function creates tables that point to an external location.
  This specifies that the created Hive tables do not manage the underlying data.

  If specified otherwise,
  """
  @spec format_sql_create_table(Etl.Table.t()) :: String.t()
  def format_sql_create_table(table) do
    column_str =
      format_columns(table)
      |> reindent(2)

    with_str =
      format_sql_with(table)
      |> reindent(2)

    ~s{CREATE TABLE IF NOT EXISTS #{format_location(table)} (
#{column_str}
) WITH (
#{with_str}
)}
  end

  def format_presto_use_statement(table) do
    ~s{USE #{table.catalog}.#{table.schema}}
  end

  @spec format_presto_sync_partitions(Etl.Table.t(), keyword()) :: String.t()

  def format_presto_sync_partitions(table, opts) do
    sync_type = Keyword.get(opts, :sync_type, "FULL")

    statement =
      [table.schema, table_name(table), sync_type]
      |> Enum.map(fn s -> "'" <> s <> "'" end)
      |> Enum.join(", ")

    ~s{CALL system.sync_partition_metadata(#{statement})}
  end

  @spec write_table(Etl.Table.t(), keyword()) :: term()

  def write_table(table, opts) do
    # If set, attempt to drop the table before recreating and syncing it.
    #
    # Also changes the syncing behaviour to either drop and load all paritions
    # or ony add new partitions.
    drop_table? = Keyword.get(opts, :drop_table?)

    sync_type =
      if drop_table? do
        "FULL"
      else
        "ADD"
      end

    drop_statement = format_sql_drop_table(table)
    create_statement = format_sql_create_table(table)
    use_statement = format_presto_use_statement(table)
    sync_statement = format_presto_sync_partitions(table, sync_type: sync_type)

    statements =
      if drop_table? do
        Logger.info(
          "dropping table " <>
            IO.ANSI.blue() <>
            table_name(table) <>
            IO.ANSI.reset()
        )

        [
          drop_statement,
          create_statement,
          use_statement,
          sync_statement
        ]
      else
        [
          create_statement,
          use_statement,
          sync_statement
        ]
      end

    Logger.info(
      "writing table " <>
        IO.ANSI.blue() <>
        table_name(table) <>
        IO.ANSI.reset()
    )
    IO.puts("statements: #{statements}")

    Presto.session(catalog: table.catalog)
    |> Presto.query(statements)
  end

  defp reindent(s, indent) do
    s
    |> String.split("\n")
    |> Enum.map(fn s -> String.duplicate(" ", indent) <> s end)
    |> Enum.join("\n")
  end

  @spec process_map(Etl.Table.t(), map()) :: map()

  def process_map(table, map) do
    column_map = Map.new(table.columns)

    map
    # Create a new map with only the valid columns from the original map.
    |> (&:maps.with(:maps.keys(column_map), &1)).()
    # Add any missing columns.
    |> (&:maps.merge(:maps.map(fn _, _ -> nil end, column_map), &1)).()
    # Coerce column values to appropriate types.
    |> (&:maps.map(
          fn k, v ->
            column_map
            |> Map.get(k)
            |> cast_column(v)
          end,
          &1
        )).()
    # Filter out any nil values.
    |> (&:maps.filter(fn _, v -> v != nil end, &1)).()
  end

  @spec cast_column(String.t(), term()) :: term()

  # Turn any value into a string using Kernel.inspect.
  def cast_column("VARCHAR" <> _, nil), do: nil

  def cast_column("VARCHAR" <> _, v) when not is_binary(v), do: Kernel.inspect(v)

  # Cast a string to an integer.
  def cast_column("BIGINT", v) when is_binary(v), do: String.to_integer(v)

  def cast_column("BIGSIZE", nil), do: -1

  def cast_column("BIGSIZE", v) when is_binary(v), do: String.to_integer(v)

  def cast_column("TIMESTAMP", v) when is_binary(v), do: Fishbowl.assume_utc(v)

  # Default unit is :millisecond.
  def cast_column("TIMESTAMP", v) when is_integer(v), do: Fishbowl.assume_utc(v)

  def cast_column("TIMESTAMP_μs", v) when is_float(v) do
    (v * 1_000_000)
    |> trunc()
    |> Fishbowl.assume_utc(unit: :microsecond)
  end

  # In the default case, don't do anything to the incoming pair.
  def cast_column(_, v), do: v
end
