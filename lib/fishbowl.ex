defmodule Fishbowl do
  @moduledoc """
  Utility functions that are generally useful for the data loaders.
  """

  @doc """
  Filter out failures, while unwrapping successful results.

  ## Examples

      iex> Fishbowl.keep_ok([{:ok, "foo"}, {:error, "whoops"}, {:ok, "bar"}]) |> Enum.to_list
      ["foo", "bar"]

  """
  @spec keep_ok(Enumerable.t()) :: Enumerable.t()
  def keep_ok(stream) do
    stream
    |> Stream.flat_map(fn
      {:ok, result} -> [result]
      _ -> []
    end)
  end

  @doc """
  Convert a formatted timestamp string without a timezone into a DateTime as if
  it were UTC. If an integer value is provided, default to unix epoch
  milliseconds and allow for a user-provided option to specify the unit.

  ## Examples

      iex> Fishbowl.assume_utc("2020-02-01T10:01:02")
      ~U[2020-02-01 10:01:02Z]

      iex> Fishbowl.assume_utc(1_000)
      ~U[1970-01-01 00:00:01.000Z]

      iex> Fishbowl.assume_utc(1_000, unit: :second)
      ~U[1970-01-01 00:16:40Z]

  """

  @spec assume_utc(String.t()) :: DateTime.t()

  def assume_utc(s) when is_binary(s) do
    s
    |> NaiveDateTime.from_iso8601!()
    |> DateTime.from_naive!("Etc/UTC")
  end

  @spec assume_utc(integer()) :: DateTime.t()

  @spec assume_utc(integer(), keyword()) :: DateTime.t()

  def assume_utc(s, opts \\ []) when is_integer(s) do
    s
    |> DateTime.from_unix!(Keyword.get(opts, :unit, :millisecond))
  end

  @spec windowed_sort(
          Enumerable.t(),
          (Stream.element(), Stream.element() -> boolean()),
          pos_integer()
        ) :: Enumerable.t()
  def windowed_sort(enum, compare, buffer_size) do
    {:ok, remaining} = Agent.start_link(fn -> [] end)

    sorted_prefix =
      Stream.transform(
        enum,
        fn -> {Heap.new(compare), remaining} end,
        fn elem, {heap, remaining} ->
          if Heap.size(heap) < buffer_size do
            {[], {Heap.push(heap, elem), remaining}}
          else
            {root, heap} = Heap.split(heap)
            heap = Heap.push(heap, elem)
            {[root], {heap, remaining}}
          end
        end,
        fn {heap, remaining} ->
          Agent.update(remaining, fn _ -> Enum.into(heap, []) end)
        end
      )

    Stream.run(sorted_prefix)
    Stream.concat(sorted_prefix, Agent.get(remaining, fn state -> state end))
  end

  @doc """
  Attempts to extract an encoded date string from `path`.

  Optionally allows the caller to specify the regex `patterns`.
  The default `patterns` search for:

  - year: "YYYY"
  - month: "MM"
  - day: "DD"

  delimited by '/', '-', or '_'.

  ## Examples

      iex> Fishbowl.infer_date_from_path("/bin/ls")
      nil

      iex> Fishbowl.infer_date_from_path("with-dashes")
      nil

  """
  @spec infer_date_from_path(path :: String.t()) :: DateTime.t() | nil

  def infer_date_from_path(path) do
    infer_date_from_path(
      path,
      [
        "(?<year>\\d{4})",
        "(?<month>\\d{2})",
        "(?<day>\\d{2})"
      ]
    )
  end

  @spec infer_date_from_path(path :: String.t(), patterns :: [String.t()]) :: DateTime.t() | nil

  def infer_date_from_path(path, pattern) do
    ["/", "-", "_"]
    |> Enum.map(fn delim ->
      regex =
        pattern
        |> Enum.join(delim)
        |> (fn pattern -> ~r|^.*#{pattern}.*| end).()

      infer_date_from_path_regex(path, regex)
    end)
    |> Enum.reject(&(&1 == nil))
    |> (&(if Enum.empty?(&1) do
            nil
          else
            List.first(&1)
          end)).()
  end

  defp infer_date_from_path_regex(path, regex) do
    # Set default values for month and day but return if no year is found.

    regex
    |> Regex.named_captures(path)
    |> (fn x -> x || %{} end).()
    |> Map.update("month", "01", &Function.identity/1)
    |> Map.update("day", "01", &Function.identity/1)
    |> case do
      %{"year" => year, "month" => month, "day" => day} ->
        Enum.join([year, month, day], "-")
        |> Kernel.<>("T00:00:00")
        |> assume_utc

      _ ->
        nil
    end
  end

  @spec env_prepend(String.t()) :: String.t()

  def env_prepend(value) do
    env =
      System.get_env("FISHBOWL_ENVIRONMENT", "dev")
      |> String.downcase()

    if env in ["prod", "production"] do
      value
    else
      "#{env}_#{value}"
    end
  end

  @doc """
  Get the file extension from a given file `path`. Includes a '.' prefix character.

  Assumes that the '.' characters are exclusively used to denote file extension.

  ## Examples

  iex> Fishbowl.ext("a/b/c")
  ...> ""

  iex> Fishbowl.ext("a/b/c.json")
  ...> ".json"

  iex> Fishbowl.ext("s3://a/b/c.csv.gz")
  ...> ".csv.gz"

  iex> Fishbowl.ext("a/b/.c")
  ...> ""

  iex> Fishbowl.ext("a/b/.c.log")
  ...> ".log"

  iex> Fishbowl.ext("a.b.c.log")
  ...> ".b.c.log"
  """

  @spec ext(String.t()) :: String.t()

  def ext("." <> path), do: ext(path)

  def ext(path) do
    path
    |> Path.basename()
    |> String.split(".")
    |> Enum.drop(1)
    |> Enum.join(".")
    |> (&Kernel.<>(".", &1)).()
  end
end
