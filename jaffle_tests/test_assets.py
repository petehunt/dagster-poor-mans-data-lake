from jaffle.assets import continent_population, population
from jaffle.duckpond import DuckDB


def test_assets():
    p = population()
    c = continent_population(p)
    assert (
        c.sql
        == "select continent, avg(pop_change) as avg_pop_change from $population group by 1 order by 2 desc"
    )
    assert "population" in c.bindings
    df = DuckDB().query(c)
    top = df.loc[0]
    assert top["continent"] == "Africa"
    assert round(top["avg_pop_change"]) == 2
