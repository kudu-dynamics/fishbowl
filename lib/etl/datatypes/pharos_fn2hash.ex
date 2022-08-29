defmodule Etl.Datatype.PharosFn2hash do
  @moduledoc """
  Extract information from data output from CMU SEI's Pharos fn2hash utility.

  ---

  For documentation, see:

  https://github.com/cmu-sei/pharos/blob/master/tools/fn2hash/fn2hash.pod
  """
  use(
    Etl.Datatype,
    columns: [
      {"composite_pic_hash", "VARCHAR"},
      {"exact_hash", "VARCHAR"},
      {"file_md5", "VARCHAR"},
      {"fn_addr", "VARCHAR"},
      {"mnemonic_category_counts_hash", "VARCHAR"},
      {"mnemonic_category_hash", "VARCHAR"},
      {"mnemonic_count_hash", "VARCHAR"},
      {"mnemonic_hash", "VARCHAR"},
      {"num_basic_blocks", "BIGSIZE"},
      {"num_basic_blocks_in_cfg", "BIGSIZE"},
      {"num_bytes", "BIGSIZE"},
      {"num_instructions", "BIGSIZE"},
      {"pic_hash", "VARCHAR"},

      # DEV: This field is extraneous to the original data source and must be
      #      inserted manually.
      {"file_sha256", "VARCHAR(64)"}
    ],
    version: 1
  )

  def to_rows(%{"analysis" => data}) when is_map(data) do
    to_rows(data)
  end

  def to_rows(data) when is_map(data) do
    data
    |> (&Map.put_new(&1, "file_md5", elem(Map.pop(&1, "filemd5"), 0))).()
    |> (&Map.put_new(&1, "file_sha256", elem(Map.pop(&1, "sha256"), 0))).()
    |> List.wrap()
  end

  def to_rows(_), do: []
end
