# Fishbowl ETL

```mermaid

flowchart TD
  subgraph stage0[select_and_download]
    s0_0["Clients.S3.list_below_prefix(bucket, prefix)"]

    s0_1_0["Clients.S3.download_temp()"]
    s0_1_1["Clients.S3.download_temp()"]
    s0_1_2["Clients.S3.download_temp()"]

    s0_out["Downloaded File Paths"]

    s0_0 -.-> s0_1_0 & s0_1_1 & s0_1_2
    s0_1_0 & s0_1_1 & s0_1_2 -.-> s0_out
  end
  stage0 ===> stage1

  subgraph stage1[load_and_parse]
    s1_0_0["File.stream!([:compressed])"]
    s1_0_1["File.stream!([:compressed])"]
    s1_0_2["File.stream!([:compressed])"]

    s1_1_0["CSV.decode()"]
    s1_1_1["Jason.decode!()"]
    s1_1_2["Stream.map(&Jason.decode!/1)"]

    s1_out["Decoded Rows"]

    s1_0_0 -.-> s1_1_0
    s1_0_1 -.-> s1_1_1
    s1_0_2 -.-> s1_1_2
    s1_1_0 & s1_1_1 & s1_1_2 -.-> s1_out
  end
  stage1 ===> stage2

  subgraph stage2[target_transform]
    s2_0_0["Etl.Datatype.IsiIpAddress.to_rows()"]
    s2_0_1["Etl.Datatype.PurifierBasic.to_rows()"]
    s2_0_2["Etl.Datatype.ZeekDns.to_rows()"]

    s2_out["All Transformed Rows"]

    s2_0_0 & s2_0_1 & s2_0_2 -.-> s2_out
  end
  stage2 ===> stage3

  subgraph stage3[partition_by_datatype]
    s3_0_0["[IsiIpAddress Row Batch]"]
    s3_0_1["[PurifierBasic Row Batch]"]
    s3_0_2["[ZeekDns Row Batch]"]

    s3_1_0["[IsiIpAddress Parquet]"]
    s3_1_1["[PurifierBasic Parquet]"]
    s3_1_2["[ZeekDns Parquet]"]

    s3_out["All Parquet Files"]

    s3_0_0 -.-> s3_1_0
    s3_0_1 -.-> s3_1_1
    s3_0_2 -.-> s3_1_2
    s3_1_0 & s3_1_1 & s3_1_2 -.-> s3_out
  end
  stage3 ===> stage4

  subgraph stage4[emit_output]
    s4_0_0["Clients.S3.upload(parquet, bucket, key)"]
  end
  stage4 ===> stage5

  subgraph stage5[write_tables]
  end

```

## select_and_download

Takes as input a data source location. Could be a file path, S3 path, or a Redis instance, etc.

The files from the selected data source are turned into a flow wherein each file is downloaded to a temporary directory.

## load_and_parse

Each file in the input flow is loaded as a potentially compressed file stream.

The streams get decoded by either inferring the type from the path (e.g. "test.json.gz" -> :json, "test.csv.gz" -> :csv) or by using a target's default configured datatype.

Outputs a flow of parsed data rows.

## target_transform

Using the given target's list of datatypes, each datatype is flat mapped against the incoming stream of rows.

Datatypes can drop rows not intended for them and generate more rows than originally present.
`N` rows are transformed into `N * D` rows where `D` is the number of applicable datatypes.

## partition_by_datatype

The incoming flow of transformed rows is partitioned at this stage into separate swim lanes in preparation to write datatype-specific Parquet files.

Rows tagged by datatype are aggregated into chunks of `1_000_000` rows.

## emit_output

Each Parquet file is uploaded to S3 or written to disk depending on user-provided options.

## write_tables

Tables are written to Presto depending on which datatypes emitted Parquet files.
