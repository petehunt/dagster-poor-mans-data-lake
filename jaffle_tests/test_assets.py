from jaffle.assets import cereals, mfg_popularity
from jaffle.duckpond import DuckDB


def test_assets():
    c = cereals()
    p = mfg_popularity(c)
    assert (
        p.sql
        == "select mfr, count(*) as num_cereals from $cereals group by 1 order by 2 desc"
    )
    assert "cereals" in p.bindings
    df = DuckDB().query(p)
    top = df.loc[0]
    assert top["mfr"] == "K"
    assert top["num_cereals"] == 23
