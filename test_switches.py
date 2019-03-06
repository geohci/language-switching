from session_utils import get_lang_switch
from session_utils import get_nonlang_switch
from session_utils import Pageview, Session

# NOTE: for testing, it's okay to reorder these page views even though the times no longer make sense then
p1 = Pageview(dt='2019-02-16T11:31:53', proj='enwiki', title='Columbidae', wd='Q10856')
p2 = Pageview(dt='2019-02-16T11:32:05', proj='enwiki', title='Anarchism', wd='Q6199')
p3 = Pageview(dt='2019-02-16T11:32:13', proj='eswiki', title='Columbidae', wd='Q10856')

def session_with_enwikifrom_switches():
    return Session("USER_ENWIKIFROM_SWITCHES", "COUNTRY", [p1, p2, p3])

def session_with_enwikito_switches():
    return Session("USER_ENWIKITO_SWITCHES", "COUNTRY", [p3, p1, p2])

def session_with_no_switches():
    return Session("USER_NO_SWITCHES", "COUNTRY", [p2, p3])

def main():
    assert get_lang_switch(pvs=session_with_enwikifrom_switches().pageviews, wikidbs=("enwiki",)) == [(0,2)]
    assert get_lang_switch(pvs=session_with_enwikito_switches().pageviews, wikidbs=("enwiki",)) == [(0,1)]
    assert get_lang_switch(pvs=session_with_no_switches().pageviews, wikidbs=("enwiki",)) == []

    assert get_nonlang_switch(pvs=session_with_enwikifrom_switches().pageviews,
                              wikidb="enwiki",
                              direction="from") == [1]
    assert get_nonlang_switch(pvs=session_with_enwikifrom_switches().pageviews,
                              wikidb="enwiki",
                              direction="to") == []
    assert get_nonlang_switch(pvs=session_with_enwikifrom_switches().pageviews,
                              wikidb="eswiki",
                              direction="from") == []
    assert get_nonlang_switch(pvs=session_with_enwikifrom_switches().pageviews,
                              wikidb="eswiki",
                              direction="to") == []

    assert get_nonlang_switch(pvs=session_with_enwikito_switches().pageviews,
                              wikidb="enwiki",
                              direction="to") == [2]
    assert get_nonlang_switch(pvs=session_with_enwikito_switches().pageviews,
                              wikidb="enwiki",
                              direction="from") == []
    assert get_nonlang_switch(pvs=session_with_enwikito_switches().pageviews,
                              wikidb="eswiki",
                              direction="to") == []
    assert get_nonlang_switch(pvs=session_with_enwikito_switches().pageviews,
                              wikidb="eswiki",
                              direction="from") == []

    assert get_nonlang_switch(pvs=session_with_no_switches().pageviews,
                              wikidb="enwiki",
                              direction="from") == []
    assert get_nonlang_switch(pvs=session_with_no_switches().pageviews,
                              wikidb="enwiki",
                              direction="to") == []
    assert get_nonlang_switch(pvs=session_with_no_switches().pageviews,
                              wikidb="eswiki",
                              direction="from") == []
    assert get_nonlang_switch(pvs=session_with_no_switches().pageviews,
                              wikidb="eswiki",
                              direction="to") == []


if __name__ == "__main__":
    main()