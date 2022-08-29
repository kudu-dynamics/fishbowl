defmodule Etl.Target.StageZero do
  @moduledoc """
  This stage is responsible for selecting and downloading the files to process.

  Returns a flow: [<file path>, ...]
  """

  @doc """
  Locate usable files on the given path and emit the paths as a flow.
  """

  @spec flow_from_local(String.t()) :: Flow.t()

  def flow_from_local(path) do
    # Locate usable files on the given path.
    # Emit flow of file paths.
    case File.ls(path) do
      {:ok, paths} -> paths |> Enum.map(fn p -> "#{path}/#{p}" end) 
      {:error, :enotdir} -> [path]
      {:error, :enoent} -> []
    end
    # Convert the list of files to a flow.
    |> Flow.from_enumerable()
  end

  @doc """
  Attempt to load data from objects in S3. Emit a flow of paths to the
  downloaded objects.
  """

  @spec flow_from_s3(String.t()) :: Flow.t()

  def flow_from_s3(path) do
    # Attempt to load data from objects in S3.
    # Emit flow of file paths.
    [bucket, prefix] = Clients.S3.split_path(path)

    # Get a listing of objects in S3.
    #
    # Treat the prefix as a prefix if it ends with a '/' character.
    if String.ends_with?(prefix, "/") do
      prefix
      |> String.replace_suffix("/", "")
      |> Clients.S3.list_below_prefix(bucket)
    else
      [{:key, prefix}]
      |> Map.new()
      |> List.wrap()
    end
    # Convert the list of objects to a flow.
    |> Flow.from_enumerable()
    # Download each file in the listing.
    |> Flow.map(fn listing -> Clients.S3.download_temp(bucket, listing[:key]) end)
  end

  @doc """
  Load a list of file paths from a uri-specified backend as a Flow of file
  paths where the files are downloaded on demand.
  """

  @spec select_and_download(String.t()) :: Flow.t()

  def select_and_download(path) do
    # Load a list of file paths to process into a flow.
    case path do
      "s3://" <> path -> flow_from_s3(path)
      "file://" <> path -> flow_from_local(path)
      path -> flow_from_local(path)
    end
  end
end
