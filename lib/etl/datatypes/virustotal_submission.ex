defmodule Etl.Datatype.VirustotalSubmission do
  @moduledoc """
  Submission data extracted from VirusTotal scan reports.
  """
  alias Etl.Datatype.Virustotal

  use(
    Etl.Datatype,
    columns: [
      {"file_sha256", "VARCHAR(64)"},
      {"submission_filename", "VARCHAR"},
      {"submission_interface", "VARCHAR"},
      {"submitter_city", "VARCHAR"},
      {"submitter_country", "VARCHAR"},
      {"submitter_id", "VARCHAR"},
      {"submitter_region", "VARCHAR"},
      {"timestamp", "VARCHAR"}
    ],
    partitions: [
      {"year", "VARCHAR(4)"},
      {"month", "VARCHAR(2)"}
    ],
    partition_with_date: false,
    version: 1
  )

  @doc """
  ## Examples

      iex> Etl.Datatype.VirustotalSubmission.to_rows(%{
      ...>   "scan_id" => "example_sha256-example_timestamp",
      ...>   "submission" => %{
      ...>     "filename" => "example_filename",
      ...>     "submitter_city" => "example_city",
      ...>     "submitter_country" => "example_country",
      ...>     "submitter_id" => "example_id",
      ...>     "submitter_region" => "example_region",
      ...>     "date" => "2000-01-01 00:00:00",
      ...>     "interface" => "api"
      ...>   }
      ...> })
      [%{
         "date" => "2000-01-01 00:00:00",
         "file_sha256" => "example_sha256",
         "submission_filename" => "example_filename",
         "submission_interface" => "api",
         "submitter_city" => "example_city",
         "submitter_country" => "example_country",
         "submitter_id" => "example_id",
         "submitter_region" => "example_region",
         "timestamp" => "example_timestamp"
      }]

      iex> Etl.Datatype.VirustotalSubmission.to_rows(%{
      ...>   "scan_id" => "example_sha256-example_timestamp",
      ...>   "submission" => %{}
      ...> })
      []
  """

  def to_rows(%{
        "file_sha256" => file_sha256,
        "submission" => submission,
        "timestamp" => timestamp
      })
      when is_map(submission) and map_size(submission) > 0 do
    submission
    |> Map.put("file_sha256", file_sha256)
    |> Map.put("timestamp", timestamp)
    |> map_rename("filename", "submission_filename")
    |> map_rename("interface", "submission_interface")
    |> List.wrap()
  end

  def to_rows(data) when is_map(data) and map_size(data) > 0 do
    data
    |> Virustotal.parse_scan_id()
    |> to_rows()
  end

  def to_rows(_), do: []

  defp map_rename(data, old_key, new_key) do
    data
    |> Map.pop(old_key)
    |> (fn {v, data} -> data |> Map.put(new_key, v) end).()
  end
end
