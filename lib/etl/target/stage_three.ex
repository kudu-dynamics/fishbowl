defmodule Etl.Target.StageThree do
  @moduledoc """
  This stage partitions the incoming flow into files that group rows by both
  datatype and partition column values.

  Returns a flow: [{<file path>, module()}, ...]
  """
  require Logger
  use Memoize

  @ets_table :target_stagethree_files

  defmemo file_handle(key, module) do
    # Buffer writes until 8192 bytes have accumulated or 100 ms have passed.
    {:ok, file} = File.open(key, [:append, {:delayed_write, 8192, 100}])

    # Track the newly opened file for clean up later.
    if :ets.insert_new(@ets_table, {key, {file, module}}) == true do
      file
    else
      # If the file has already been opened, close our file descriptor and
      # lookup the previously registered one.
      File.close(file)

      [{^key, {file, _}}] = :ets.lookup(@ets_table, key)

      file
    end
  end

  @doc """
  ## Examples

      iex> Etl.Target.StageThree.format_row_partition(
      ...>   %{"date" => "1970-01-01", "source" => "internet"},
      ...>   Etl.Datatype.ZeekDns
      ...> )
      "dev_zeek_dns__1__date=1970-01-01__source=internet"
  """

  @spec format_row_partition(map(), module()) :: String.t()

  def format_row_partition(row, module) do
    [
      module.get_table() |> Etl.Table.format_storage_path()
      | module.get_table().partitions
        |> Enum.reduce([], fn k, acc ->
          v =
            row
            |> Map.get(k, nil)
            |> (&Etl.Table.cast_column("VARCHAR", &1)).()

          ["#{k}=#{v}" | acc]
        end)
        |> Enum.reverse()
    ]
    |> Enum.join("/")
    |> String.replace("/", "__")
  end

  @spec partition_by_datatype(Flow.t(), module()) :: Flow.t()

  def partition_by_datatype(flow, module) do
    # Get a map from datatype string to module.
    # Each incoming row will have a "datatype" key that can be used to find the
    # appropriate module.
    datatypes_map =
      module.get_datatypes()
      |> Enum.map(&{&1.get_datatype(), &1})
      |> Map.new()

    # Create a temporary directory to house the reconstructed files.
    {:ok, dir_path} = Temp.mkdir()

    # Create an ETS table to track the newly created files.
    _ = :ets.new(@ets_table, [:named_table, :public, :set])

    # Write all incoming rows to the appropriate files.
    flow
    |> Flow.map(fn {datatype, row} ->
      module = Map.get(datatypes_map, datatype)
      partition_key = format_row_partition(row, module)

      row
      |> Jason.encode()
      |> case do
        {:ok, json} ->
          # Append the row to the appropriate file.
          "#{dir_path}/#{partition_key}.jsonl"
          |> file_handle(module)
          |> IO.binwrite(json <> "\n")

        _ ->
          Logger.warn("Bad row for #{datatype}: #{Kernel.inspect(row)}")

          :ok
      end

      :ok
    end)
    |> Flow.run()

    Logger.info("stage_three complete")

    # Close each of the open files and pass along an enumerable of the file
    # paths.
    @ets_table
    |> :ets.tab2list()
    |> Enum.flat_map(fn {path, {file, module}} ->
      File.close(file)

      [{path, module}]
    end)
    |> Flow.from_enumerable()
  end
end
