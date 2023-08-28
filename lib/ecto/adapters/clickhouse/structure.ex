defmodule Ecto.Adapters.ClickHouse.Structure do
  @moduledoc false
  alias Ch.Query
  alias Ch.Connection, as: Conn

  @conn Ecto.Adapters.ClickHouse.Connection

  def structure_load(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    {cmd, cmd_args} = clickhouse_client_cmd()

    case run_with_cmd(cmd, cmd_args ++ ["--queries-file", path], config) do
      {_output, 0} -> {:ok, path}
      {output, _} -> {:error, output}
    end
  end

  def structure_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    migration_source = config[:migration_source] || "schema_migrations"
    database = config[:database] || "default"

    with {:ok, conn} <- Conn.connect(config),
         {:ok, tables, conn} <- show("TABLES", conn),
         {:ok, dicts, conn} <- show("DICTIONARIES", conn),
         tables = tables -- [migration_source],
         {:ok, tables, conn} <- show_create("TABLE", conn, [migration_source | tables]),
         {:ok, dicts, conn} <- show_create("DICTIONARY", conn, dicts),
         {:ok, versions, _conn} <- dump_versions(conn, database, migration_source) do
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, [tables, dicts, versions])
      {:ok, path}
    end
  end

  defp show(what, conn) do
    with {:ok, %{rows: rows}, conn} <- exec(conn, "SHOW #{what}") do
      objects = Enum.map(rows, fn [object] -> object end)
      {:ok, objects, conn}
    end
  end

  defp show_create(what, conn, objects) do
    show = fn object -> "SHOW CREATE #{what} #{@conn.quote_name(object)}" end

    result =
      Enum.reduce_while(objects, {[], conn}, fn object, {schemas, conn} ->
        case exec(conn, show.(object)) do
          {:ok, %{rows: [[schema]]}, conn} -> {:cont, {[schema, ";\n\n" | schemas], conn}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      {schemas, conn} when is_list(schemas) -> {:ok, schemas, conn}
    end
  end

  defp dump_versions(conn, database, table) do
    table = @conn.quote_table(database, table)
    stmt = "SELECT * FROM #{table} FORMAT Values"

    with {:ok, %{rows: rows}, conn} <- exec(conn, stmt) do
      rows = rows |> IO.iodata_to_binary() |> String.replace("),(", "),\n(")
      versions = ["INSERT INTO ", table, " (version, inserted_at) VALUES\n", rows, ";\n"]
      {:ok, versions, conn}
    end
  end

  def exec(conn, sql, params \\ [], opts \\ []) do
    query = Query.build(sql)
    params = DBConnection.Query.encode(query, params, [])

    case Conn.handle_execute(query, params, opts, conn) do
      {:ok, query, result, conn} -> {:ok, DBConnection.Query.decode(query, result, []), conn}
      {:disconnect, reason, _conn} -> {:error, reason}
      {:error, reason, _conn} -> {:error, reason}
    end
  end

  defp clickhouse_client_cmd do
    candidates = [
      {"clickhouse-client", _args = []},
      {"clickhouse", _args = ["client"]}
    ]

    cmd_with_args = Enum.find(candidates, fn {cmd, _args} -> System.find_executable(cmd) end)

    cmd_with_args ||
      raise "could not find `clickhouse-client` nor `clickhouse` executables in path, " <>
              "please guarantee that one of them is available before running ecto commands"
  end

  defp run_with_cmd(cmd, cmd_args, opts) do
    args = ["--host", opts[:hostname] || "localhost"]
    args = if username = opts[:username], do: ["--user", username | args], else: args
    args = if password = opts[:password], do: ["--password", password | args], else: args
    args = if port = opts[:port], do: ["--port", to_string(port) | args], else: args
    args = if database = opts[:database], do: ["--database", database | args], else: args

    System.cmd(cmd, cmd_args ++ args, stderr_to_stdout: true)
  end
end
