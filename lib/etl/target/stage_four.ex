defmodule Etl.Target.StageFour do
  @moduledoc """
  This stage takes incoming file paths of processed row data, converts files to
  Parquet, and uploads them to S3.

  Returns a flow: [{<file path>, module()}, ...]
  """

  # XXX replace with defaults
  @defaults %{
    :dst_bucket => "<default_dst_bucket>",
    :src_bucket => "<default_src_bucket>"
  }

  @spec emit_output(Flow.t(), keyword()) :: Flow.t()

  def emit_output(flow, opts) do
    s3_bucket = @defaults |> Map.get(:dst_bucket)
    start_ms = Keyword.get(opts, :start_ms, 0)
    upload? = Keyword.get(opts, :upload?)

    flow
    |> Flow.map(fn {path, module} ->
      
      # Convert the incoming JSON lines file to Parquet.
      data =
        path
        |> Fishbowl.Parquet.encode_file()

      stop_ms = :os.system_time(:millisecond)
      object_name = Enum.join([start_ms, stop_ms], "-") <> ".parquet"

      path =
        path
        |> Path.basename(".jsonl")

      if upload? do
        table_name = Keyword.get(opts, :table_name, nil)
        module_table = module.get_table()
        module_table_name = Map.get(module_table, :name)
        # Upload the Parquet file to S3.
        s3_key =
          [
            String.replace(path, "__", "/"),
            object_name
          ]
          |> Enum.join("/")
        
        s3_key = if table_name do
                String.replace(s3_key, module_table_name, table_name)
            else
                s3_key
            end 

        data
        |> Clients.S3.upload(s3_bucket, s3_key)
      else
        # Write the Parquet files to the current working directory.
        [path, object_name]
        |> Enum.join("__")
        |> File.write!(data)
      end

      # Pass along the stripped file name.
      {path, module}
    end)
  end
end
