defmodule Etl.Main do
  # Automatically discover the available target modules.
  @module_prefix "Elixir.Etl.Target."

  # XXX: This doesn't consistently get populated for some reason.
  @compile {:autoload, false}

  @targets :code.all_available()
           |> Enum.filter(fn {name, _, _} ->
             name
             |> to_string()
             |> String.starts_with?(@module_prefix)
           end)
           |> Enum.reject(fn {name, _, _} ->
             name
             |> to_string()
             |> String.starts_with?(@module_prefix <> "Stage")
           end)
           |> Enum.map(fn {name, _, _} ->
             {
               name
               |> to_string()
               |> String.trim_leading(@module_prefix)
               |> Recase.to_snake(),
               name
               |> to_string()
               |> String.to_existing_atom()
             }
           end)
           |> Map.new()

  @targets_docstr @targets
                  |> Map.keys()
                  |> Enum.sort()
                  |> Enum.map(fn target -> "  #{target}" end)
                  |> Enum.join("\n")

  @moduledoc """
             Fishbowl - Toolkit for processing S3 + Presto ETL pipelines.

             Usage:
               fishbowl {-t TARGET|--target TARGET} [OPTIONS] <path>
               fishbowl {-t TARGET|--target TARGET} [OPTIONS] file://<path>
               fishbowl {-t TARGET|--target TARGET} [OPTIONS] s3://<s3path>
               fishbowl -h | --help

             Options:
               -h --help           Show this screen.
               --drop-partition    If set, uploading to a data partition will first drop any existing Parquet files.
               --drop-table        If set, writing a table will first drop the existing table before recreating it.
               --profile           Whether or not to enable and display profiling information.
               --upload            Upload the final artifacts to S3. Unless set artifacts are dumped to the current
                                   working directory.
               -t --target TARGET  Run a particular ETL pipeline.

             Environment Variables:
               AWS_ACCESS_KEY_ID     
               AWS_ENDPOINT_URL      
               AWS_SECRET_ACCESS_KEY 
               FISHBOWL_ENVIRONMENT  - if not set to "prod" or "production",
                                       prepends value (default: "dev") to table names
               PRESTO_ENDPOINT       

             Targets:
             """ <> @targets_docstr

  # Force compilation order such that this module will always see the available
  # target modules.
  require Etl.Target
  require Logger
  use Application

  def start(_type, _args) do
    children = []
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  # Public API

  def main(args \\ []) do
    start_apps()

    Logger.configure(level: :info, utc_log: true)

    case configure() do
      [] ->
        :ok

      errors ->
        Enum.each(errors, fn error -> IO.puts("ERROR: #{error}") end)
        IO.puts("")
        print_help()
    end

    args
    |> parse_args()
    |> process_args()
  end

  def start_apps do
    {:ok, _} = Application.ensure_all_started(:fishbowl)
  end

  @doc """
  Load configuration values from environment variables.

  Returns a list of error messages if any are encountered.
  """
  @spec configure :: [String.t()]
  def configure do
    configure_s3() ++ configure_presto()
  end

  # Private API

  defp configure_s3 do
    # Point the S3 library at the appropriate S3 endpoint.
    [scheme, host, port] =
      System.get_env("AWS_ENDPOINT_URL", "http://localhost:9000")
      |> String.split(":")
      |> Enum.map(&String.replace(&1, "//", ""))
      |> case do
        [scheme = "http", host] -> [scheme, host, "80"]
        [scheme = "https", host] -> [scheme, host, "443"]
        [scheme, host, port] -> [scheme, host, port]
      end

    Application.put_env(
      :ex_aws,
      :s3,
      ExAws.Config.new(:s3,
        host: host,
        port: port,
        scheme: scheme <> "://"
      )
    )

    []
  end

  defp configure_presto do
    # Check if the PRESTO_ENDPOINT has been set correctly.
    session? =
      Clients.Presto.session(catalog: "")
      |> Clients.Presto.query("SHOW CATALOGS")
      |> Enum.count()

    if session? == 0 do
      ["invalid PRESTO_ENDPOINT set"]
    else
      []
    end
  end

  defp parse_args(args) do
    {parsed, argv, _invalid} =
      args
      |> OptionParser.parse(
        aliases: [
          h: :help,
          t: :target
        ],
        strict: [
          help: :boolean,
          drop_partition: :boolean,
          drop_table: :boolean,
          profile: :boolean,
          target: :string,
          table_name: :string,
          upload: :boolean
        ]
      )

    IO.inspect(parsed |> Map.new())
    [parsed |> Map.new(), argv]
  end

  defp process_args([%{help: true}, _argv]) do
    print_help()
  end

  defp process_args([%{target: target}, _argv])
       when not is_map_key(@targets, target) do
    print_help()
  end

  # fishbowl {-t TARGET|--target TARGET} [OPTIONS] <path>
  # fishbowl {-t TARGET|--target TARGET} [OPTIONS] file://<path>
  # fishbowl {-t TARGET|--target TARGET} [OPTIONS] s3://<s3path>
  defp process_args([args = %{target: target}, [path]]) do
    Logger.info(
      IO.ANSI.green() <>
        "#{target} pipeline starting: #{path}" <>
        IO.ANSI.reset()
    )

    module = @targets[target]
    metadata = module.infer_metadata_from_path(path)

    opts = [
      start_ms: :os.system_time(:millisecond),
      drop_partition?: Map.get(args, :drop_partition, false),
      drop_table?: Map.get(args, :drop_table, false),
      upload?: Map.get(args, :upload, false),
      table_name: Map.get(args, :table_name, nil)
    ]

    if Map.get(args, :profile, false) do
      profiled_run(path, module, metadata, opts)
    else
      run(path, module, metadata, opts)
    end

    # Ensure that the applications all close down.
    # Important for cleaning up temporary files.
    System.stop()
  end

  defp process_args([parsed, argv]) do
    IO.inspect(parsed)
    IO.inspect(argv)
    print_help()
  end

  defp run(path, module, metadata, opts) do
    Logger.info("tracking temporary files")
    Temp.track!()

    path
    |> Etl.Target.select_and_download()
    |> Etl.Target.load_and_parse(module)
    |> Etl.Target.target_transform(module, metadata)
    |> Etl.Target.partition_by_datatype(module)
    |> Etl.Target.emit_output(opts)
    |> Etl.Target.write_tables(opts)
    |> Flow.run()

    Logger.info("cleaning up temporary files")
    Temp.cleanup()

    Logger.info(
      IO.ANSI.green() <>
        "pipeline complete" <>
        IO.ANSI.reset()
    )
  end

  defp profiled_run(path, module, metadata, opts) do
    :fprof.trace([:start, {:procs, :all}])
    run(path, module, metadata, opts)
    :fprof.trace(:stop)

    :fprof.profile()

    :fprof.analyse(
      callers: true,
      sort: :own,
      totals: true,
      details: true
    )
  end

  @spec print_help :: no_return()
  defp print_help do
    IO.puts(@moduledoc |> String.trim_trailing("\n"))
    System.halt(1)
  end
end
