defmodule Clients.S3 do
  @moduledoc """
  Utility library for interacting with AWS S3.
  """
  require Logger

  @doc """
  Splits an S3path to get the bucket and object path or prefix.

  ## Examples

      iex> Clients.S3.split_path("public/tools/s3cli/s3cli.tar.gz")
      ["public", "tools/s3cli/s3cli.tar.gz"]

      iex> Clients.S3.split_path("s3://public/tools/s3cli/s3cli.tar.gz")
      ["public", "tools/s3cli/s3cli.tar.gz"]
  """
  def split_path(path) do
    path
    |> String.trim_leading("s3://")
    |> String.trim_leading("s3a://")
    |> String.trim_leading("s3n://")
    |> String.split("/", parts: 2)
  end

  def upload(body, bucket, key) do
    Logger.debug("Uploading to s3://#{bucket}/#{key}")

    response =
      ExAws.S3.put_object(bucket, key, body)
      |> ExAws.request()

    case response do
      {:ok, result} ->
        result

      {:error, msg} ->
        Logger.error("ERROR: #{msg}")
        nil
    end
  end

  def download(key, bucket) do
    tmp_path = download_temp(key, bucket)
    data = File.read!(tmp_path)
    File.rm!(tmp_path)
    data
  end

  @spec download_stream(String.t(), String.t()) :: Enumerable.t()
  def download_stream(key, bucket) do
    download_temp(key, bucket)
    |> File.stream!()
  end

  @spec download_temp(String.t(), String.t()) :: String.t()
  def download_temp(bucket, key) do
    # Preserve the object extension if possible.
    tmp_path = Temp.path!(suffix: Fishbowl.ext(key))
    Logger.debug("Downloading s3://#{bucket}/#{key} to #{tmp_path}")
    ExAws.S3.download_file(bucket, key, tmp_path) |> ExAws.request!()
    tmp_path
  end

  @doc """
  Given an S3 prefix and bucket combination, download the objects in batches
  and serve their contents up as part of a lazy stream.

  The default behaviour is to download 256 files at a time.
  """
  @spec download_prefix(String.t(), String.t()) :: Enumerable.t()
  def download_prefix(prefix, bucket) do
    download_prefix(prefix, bucket, 256)
  end

  def download_prefix(_prefix, _bucket, batch_size) when batch_size <= 0 do
    []
  end

  @spec download_prefix(String.t(), String.t(), pos_integer()) :: Enumerable.t()
  def download_prefix(prefix, bucket, batch_size) when batch_size > 0 do
    list_below_prefix(prefix, bucket)
    |> Stream.chunk_every(batch_size)
    |> Stream.flat_map(fn batch ->
      Task.async_stream(
        batch,
        fn object -> download(object[:key], bucket) end,
        max_concurrency: batch_size,
        timeout: 1000 * 60 * 60
      )
      # XXX: Need to handle the case in which we don't successfully download a
      #      file. There should be some mechanism to reprocess them until the
      #      procedure is done.
      |> Enum.map(fn {:ok, x} -> x end)
    end)
  end

  def list_whole_bucket(bucket) do
    Logger.info("Listing all contents of s3://#{bucket}")

    ExAws.S3.list_objects_v2(bucket)
    |> ExAws.stream!()
  end

  def list_below_prefix(prefix, bucket) do
    padded_prefix = String.trim_trailing(prefix, "/") <> "/"
    Logger.info("Listing contents under s3://#{bucket}/#{padded_prefix}")

    ExAws.S3.list_objects_v2(bucket, delimiter: "/", prefix: padded_prefix)
    |> ExAws.stream!()
  end
end
