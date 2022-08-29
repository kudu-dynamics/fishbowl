defmodule Etl.Target.StageOne do
  @moduledoc """
  This stage takes in the flow of locally downloaded file paths and decodes
  their contents.

  Returns a flow: [<row: map()>, ...]
  """

  @json_decode_opts [:return_maps, :use_nil, :copy_strings]
  @load_file_infer_error "can't infer file type"

  def load_file_infer_type_or_else(path, format, opts) do
    load_file_infer_type(path, opts)
  rescue
    RuntimeError -> load_file(path, format, opts)
  end

  def load_file_infer_type(path) do
    load_file_infer_type(path, [])
  end

  def load_file_infer_type(path, opts) do
    cond do
      String.ends_with?(path, ".csv") -> load_file(path, :csv, opts)
      String.ends_with?(path, ".csv.gz") -> load_file(path, :csv, opts)
      String.ends_with?(path, ".json") -> load_file(path, :json, opts)
      String.ends_with?(path, ".json.gz") -> load_file(path, :json, opts)
      String.ends_with?(path, ".jsonl") -> load_file(path, :jsonlines, opts)
      String.ends_with?(path, ".jsonl.gz") -> load_file(path, :jsonlines, opts)
      true -> raise @load_file_infer_error
    end
  end

  @spec load_file(Path.t(), :csv | :json | :jsonlines, keyword()) :: Enumerable.t()

  def load_file(path, :csv, opts) do
    headers = Keyword.get(opts, :csv_headers, true)
    <<separator::utf8>> = Keyword.get(opts, :csv_separator, ",")

    path
    |> File.stream!([:compressed])
    |> CSV.decode(headers: headers, separator: separator)
    |> Fishbowl.keep_ok()
  end

  def load_file(path, :json, _opts) do
    path
    |> File.stream!([:compressed])
    |> Enum.join("")
    |> json_decode!()
    |> List.wrap()
  end

  def load_file(path, :jsonlines, _opts) do
    path
    |> File.stream!([:compressed])
    |> Stream.map(&json_decode/1)
    |> Fishbowl.keep_ok()
  end

  defp json_decode("") do
    nil
  end

  defp json_decode(line) do
    {:ok, :jiffy.decode(line, @json_decode_opts)}
  rescue
    exception -> {:error, exception}
  end

  defp json_decode!(line) do
    :jiffy.decode(line, @json_decode_opts)
  end

  @doc """
  Given a flow of file paths, attempt to load each file by inferring type from
  the file extension.
  If unable to do so, use the target module's default file type.
  """

  @spec load_and_parse(Flow.t(), module()) :: Flow.t()

  def load_and_parse(flow, module) do
    flow
    |> Flow.flat_map(fn path ->
      path
      |> load_file_infer_type_or_else(module.get_rawtype(), module.get_rawoptions())
    end)
  end
end
