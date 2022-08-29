defmodule Test.Clients.S3 do
  use ExUnit.Case
  doctest Clients.S3

  test "lists objects" do
    # XXX change to real bucket and data
    _bucket = "<bucket>"

    expected = %{
    }

    actual = expected

    assert expected == actual
  end
end
