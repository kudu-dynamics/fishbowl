defmodule Etl.Target do
  @moduledoc """
  Define a behaviour for target datasets for ETL.

  - ingest_raw: gather files from the data lake into an enumerable
  - upload_rollup: upload the final rolled up file (Parquet, ORC) to S3
  - write_table: generate and execute SQL to create a table in Presto
  - sync_partitions: sync partitions in Presto

  One pipeline to come from this abstraction is as follows.

  `ingest_raw |> Fishbowl.Parquet.encode() |> upload_rollup`
  `ingest_raw |> Fishbowl.Orc.encode() |> upload_rollup`
  """
  @type option_map :: map()
  @type ok_or_error ::
          :ok | Enumerable.t() | {:ok, result :: term()} | {:error, reason :: String.t()}

  @callback get_datatypes :: [module()]
  @callback get_rawoptions :: keyword()
  @callback get_rawtype :: :csv | :json | :jsonlines
  @callback infer_metadata_from_path(String.t()) :: map()
  @callback to_rows(map(), map(), map()) :: [map()]

  defdelegate select_and_download(path), to: Etl.Target.StageZero
  defdelegate load_and_parse(flow, module), to: Etl.Target.StageOne
  defdelegate load_file_infer_type(path), to: Etl.Target.StageOne
  defdelegate target_transform(flow, module, metadata), to: Etl.Target.StageTwo
  defdelegate partition_by_datatype(flow, module), to: Etl.Target.StageThree
  defdelegate emit_output(flow, opts), to: Etl.Target.StageFour
  defdelegate write_tables(flow, opts), to: Etl.Target.StageFive
end
