defmodule Etl.Datatype.Aptclass do
  @moduledoc """
  Data modeled for the APT CLASS dataset.

  ---

  For documentation, see:

  https://s2lab.kcl.ac.uk/projects/authorship/
  """
  use(
    Etl.Datatype,
    columns: [
      {"checked_sha256", "VARCHAR"},
      {"apt_country", "VARCHAR"},
      {"path", "VARCHAR"},
      {"report_hash", "VARCHAR"},
      {"apt_name", "VARCHAR"},
      {"vt_md5", "VARCHAR"},
      {"vt_sha1", "VARCHAR"},
      {"vt_file_type", "VARCHAR"}
    ],
    partition_with_date: false,
    version: 1
  )

  def to_rows(data) when is_map(data) do
    data
    |> List.wrap()
  end

  def to_rows(_), do: []
end
