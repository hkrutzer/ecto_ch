defmodule Ecto.Integration.TimestampsTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.{Account, Product}

  import Ecto.Query

  defmodule UserNaiveDatetime do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:id, Ch.Types.UInt64, autogenerate: false}
    schema "users" do
      field :name, :string
      timestamps()
    end

    def changeset(struct, attrs) do
      struct
      |> cast(attrs, [:name])
      |> validate_required([:name])
    end
  end

  defmodule UserUtcDatetime do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:id, Ch.Types.UInt64, autogenerate: false}
    schema "users" do
      field :name, :string
      timestamps(type: :utc_datetime)
    end

    def changeset(struct, attrs) do
      struct
      |> cast(attrs, [:name])
      |> validate_required([:name])
    end
  end

  test "insert and fetch naive datetime" do
    # iso8601 type
    {:ok, user} =
      %UserNaiveDatetime{id: 1}
      |> UserNaiveDatetime.changeset(%{name: "Bob"})
      |> TestRepo.insert()

    user =
      UserNaiveDatetime
      |> select([u], u)
      |> where([u], u.id == ^user.id)
      |> TestRepo.one()

    assert user
  end

  test "max of naive datetime" do
    datetime = ~N[2014-01-16 20:26:51]
    TestRepo.insert!(%UserNaiveDatetime{inserted_at: datetime})
    query = from(p in UserNaiveDatetime, select: max(p.inserted_at))
    assert [^datetime] = TestRepo.all(query)

    datetime = ~N[2014-01-16 20:26:51]
    TestRepo.insert!(%UserNaiveDatetime{inserted_at: datetime})
    query = from(p in UserNaiveDatetime, select: max(p.inserted_at))
    assert [^datetime] = TestRepo.all(query)
  end

  test "insert and fetch utc datetime" do
    # iso8601 type
    {:ok, user} =
      %UserUtcDatetime{id: 1}
      |> UserUtcDatetime.changeset(%{name: "Bob"})
      |> TestRepo.insert()

    user =
      UserUtcDatetime
      |> select([u], u)
      |> where([u], u.id == ^user.id)
      |> TestRepo.one()

    assert user
  end

  test "datetime comparisons" do
    account =
      %Account{}
      |> Account.changeset(%{name: "Test"})
      |> TestRepo.insert!()

    %Product{}
    |> Product.changeset(%{
      account_id: account.id,
      name: "Foo",
      approved_at: ~U[2023-01-01T01:00:00Z]
    })
    |> TestRepo.insert!()

    %Product{}
    |> Product.changeset(%{
      account_id: account.id,
      name: "Bar",
      approved_at: ~U[2023-01-01T02:00:00Z]
    })
    |> TestRepo.insert!()

    %Product{}
    |> Product.changeset(%{
      account_id: account.id,
      name: "Qux",
      approved_at: ~U[2023-01-01T03:00:00Z]
    })
    |> TestRepo.insert!()

    since = ~U[2023-01-01T01:59:00Z]

    assert [
             %{name: "Qux"},
             %{name: "Bar"}
           ] =
             Product
             |> select([p], p)
             |> where([p], p.approved_at >= ^since)
             |> order_by([p], desc: p.approved_at)
             |> TestRepo.all()
  end

  test "filtering with UTC datetimes" do
    time = ~U[2023-04-20 17:00:25Z]

    account =
      %Account{}
      |> Account.changeset(%{name: "Test"})
      |> TestRepo.insert!()

    %Product{}
    |> Product.changeset(%{
      account_id: account.id,
      name: "Qux1",
      approved_at: time
    })
    |> TestRepo.insert!()

    assert [%Product{approved_at: approved_at} = product] = TestRepo.all(Product)
    assert approved_at == ~N[2023-04-20 17:00:25]

    Rexbug.start("Ch.Connection :: return")
    on_exit(fn -> :timer.sleep(100) end)

    # assert TestRepo.exists?(
    #          from u in Product,
    #            where: u.approved_at == ^product.approved_at
    #        )

    assert TestRepo.exists?(
             from u in Product,
               where: fragment("? = ?", u.approved_at, ^time)
           )

    # `refute` because `DateTime` is stored as seconds, for milliseconds precision use `DateTime64(3)`
    refute TestRepo.exists?(
             from u in Product,
               where: fragment("? = ?", u.approved_at, ^DateTime.to_unix(time, :millisecond))
           )
  end
end
