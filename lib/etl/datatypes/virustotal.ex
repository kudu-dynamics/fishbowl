defmodule Etl.Datatype.Virustotal do
  @moduledoc """
  Define common utilities for Virustotal-derived datatypes.
  """

  @doc """
  All VirusTotal file reports come with a scan_id.
  The scan_id is of the format: `{file_sha256}-{timestamp}`.

  This function parses that scan_id
  """

  @spec parse_scan_id(map()) :: map() | nil

  def parse_scan_id(data = %{"scan_id" => scan_id}) when is_binary(scan_id) do
    # Parse further information from the given raw record only if a scan_id of
    # the expected format is found.
    #
    # A typical VirusTotal scan_id will be in the format:
    #
    # {file_sha256}-{timestamp}

    case String.split(scan_id, "-") do
      [file_sha256, timestamp] ->
        [
          {"file_sha256", file_sha256},
          {"timestamp", timestamp}
        ]
        |> Map.new()
        |> Map.merge(data)
        |> Map.delete("scan_id")

      # Ignore entries that do not have an appropriately formatted scan_id.
      _ ->
        nil
    end
  end

  def parse_scan_id(_), do: nil
end
