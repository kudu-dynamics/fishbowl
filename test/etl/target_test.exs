defmodule Test.Etl.Target do
  use ExUnit.Case
  doctest Etl.Target
  doctest Etl.Target.StageZero
  doctest Etl.Target.StageOne
  doctest Etl.Target.StageTwo
  doctest Etl.Target.StageThree

  test "load csv" do
    # XXX make test csv data
    actual_from_csv =
      Etl.Target.load_file_infer_type("test_data/<test_data>.csv")
      |> Enum.to_list()

    # XXX make test csv.gz data
    actual_from_gzip =
      Etl.Target.load_file_infer_type("test_data/<test_data>.csv.gz")
      |> Enum.to_list()

    expected = [
      %{
        "exact_hash" => "<exact_hash>",
        "file_md5" => "<file_md5>",
        "fn_addr" => "<fn_addr>",
        "num_basic_blocks" => 1,
        "num_basic_blocks_in_cfg" => 1,
        "num_bytes" => 1,
        "num_instructions" => 1,
        "pic_hash" => "<pic_hash>",
        "file_sha256" => "<file_sha256>"
      },
      %{
        "exact_hash" => "<exact_hash>",
        "file_md5" => "<file_md5>",
        "fn_addr" => "<fn_addr>",
        "num_basic_blocks" => 1,
        "num_basic_blocks_in_cfg" => 1,
        "num_bytes" => 1,
        "num_instructions" => 1,
        "pic_hash" => "<pic_hash>",
        "file_sha256" => "<file_sha256>"
      }
    ]

    assert expected == actual_from_csv
    assert expected == actual_from_gzip
  end

end
