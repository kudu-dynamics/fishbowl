defmodule Test.Etl.Target.PharosFn2hash do
  use ExUnit.Case
  doctest Etl.Target.PharosFn2hash

  test "loads sample csv data" do
    module = Etl.Target.PharosFn2hash
    metadata = %{}

    # add test file for pharos data
    actual =
      "test_data/<test_data_file>"
      |> Etl.Target.select_and_download()
      |> Etl.Target.load_and_parse(module)
      |> Etl.Target.target_transform(module, metadata)
      |> Enum.to_list()
      |> Enum.map(&elem(&1, 1))
      |> Enum.take(1)

    # XXX change this to be the correct test data
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
      }
    ]

    assert expected == actual
  end
end
