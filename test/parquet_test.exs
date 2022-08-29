defmodule Test.Fishbowl.Parquet do
  use ExUnit.Case
  doctest Fishbowl.Parquet

  test "call to python parquet yields parquet data" do
    example_data = [
      %{"foo" => ~U[2020-03-04 05:06:07Z]},
      %{"bar" => ~U[2020-01-02 03:04:05Z]}
    ]

    actual = Fishbowl.Parquet.encode(example_data)

    expected = "PAR1\x15\x04\x15\x10\x15\x14L\x15\x02\x15\x04\x12"

    assert expected == String.slice(actual, 0, String.length(expected))
  end

  test "call to python parquet can encode a file" do
    example_data = [
      %{"foo" => ~U[2020-03-04 05:06:07Z]},
      %{"bar" => ~U[2020-01-02 03:04:05Z]}
    ]

    path = Temp.path!()

    Enum.map(example_data, fn row ->
      row
      |> Jason.encode!()
      |> (&File.write!(path, &1 <> "\n", [:append])).()
    end)

    from_file = Fishbowl.Parquet.encode_file(path)
    File.rm(path)

    assert "PAR1" == String.slice(from_file, 0, 4)
  end
end
