defimpl Scrivener.Paginater, for: Ecto.Query do
  import Ecto.Query

  alias Scrivener.{Config, Page}

  @moduledoc false

  @spec paginate(Ecto.Query.t(), Scrivener.Config.t()) :: Scrivener.Page.t()
  def paginate(query, %Config{
        page_size: page_size,
        page_number: page_number,
        module: repo,
        caller: caller,
        options: options
      }) do
    total_entries =
      Keyword.get_lazy(options, :total_entries, fn -> total_entries(query, repo, caller, options) end)

    total_pages = total_pages(total_entries, page_size)
    page_number = min(total_pages, page_number)

    %Page{
      page_size: page_size,
      page_number: page_number,
      entries: entries(query, repo, page_number, page_size, caller),
      total_entries: total_entries,
      total_pages: total_pages
    }
  end

  defp entries(query, repo, page_number, page_size, caller) do
    offset = page_size * (page_number - 1)

    query
    |> limit(^page_size)
    |> offset(^offset)
    |> repo.all(caller: caller)
  end

  defp total_entries(query, repo, caller, options) do
    options |> IO.inspect
    if Keyword.get(options, :use_estimate_count, false) do
      total_entries_estimate(query, repo, caller)
    else
      total_entries_count(query, repo, caller)
    end
  end

  defp total_entries_count(query, repo, caller) do
    total_entries =
      query
      |> exclude(:preload)
      |> exclude(:order_by)
      |> prepare_select
      |> count
      |> repo.one(caller: caller)

    total_entries || 0
  end

  defp total_entries_estimate(query, repo, caller) do
    total_entries =
      query
      |> exclude(:preload)
      |> exclude(:order_by)
      |> prepare_select
      |> count()

    {q, b} = Ecto.Adapters.SQL.to_sql(:all, repo, total_entries)

    case Ecto.Adapters.SQL.query!(repo, "EXPLAIN (FORMAT json) #{q}", b) do
      %{rows: [[[%{"Plan" => %{"Plans" => [ %{"Plan Rows" => rows} | _]}}]]]} -> rows
      _ -> 0
    end
  end

  defp prepare_select(
         %{
           group_bys: [
             %Ecto.Query.QueryExpr{
               expr: [
                 {{:., [], [{:&, [], [source_index]}, field]}, [], []} | _
               ]
             }
             | _
           ]
         } = query
       ) do
    query
    |> exclude(:select)
    |> select([x: source_index], struct(x, ^[field]))
  end

  defp prepare_select(query) do
    query
    |> exclude(:select)
  end

  defp count(query) do
    query
    |> subquery
    |> select(count("*"))
  end

  defp total_pages(0, _), do: 1

  defp total_pages(total_entries, page_size) do
    (total_entries / page_size) |> Float.ceil() |> round
  end
end
