defmodule Clients.Presto do
  @moduledoc """
  Utility library for interacting with Presto.
  """
  require Logger

  @spec session(keyword()) :: [Prestige.Session.t()]

  def session(opts \\ []) do
    Prestige.new_session(
      url: System.get_env("PRESTO_ENDPOINT"),
      user: "unused",
      catalog: opts[:catalog]
    )
    |> List.wrap()
  end

  @spec query([Prestige.Session.t()], String.t()) :: [] | [Prestige.Session.t()]

  def query([session | _], statement) when is_binary(statement) do
    case Prestige.query(session, statement) do
      {:error, error} ->
        # Log any errors that arise and halt the current session.
        Logger.error(error.message)
        []

      _ ->
        # No error was encountered, continue with the current session.
        [session]
    end
  end

  @spec query([Prestige.Session.t()], [String.t()]) :: [] | [Prestige.Session.t()]
  def query([session | _], statements)
      when is_list(statements) and length(statements) > 0 do
    statements
    |> Enum.reduce(
      [session],
      fn statement, session -> query(session, statement) end
    )
  end

  def query([], _), do: []
end
