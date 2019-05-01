from collections import namedtuple
import csv
import gzip
import logging
import sys

csv.field_size_limit(sys.maxsize)
logging.basicConfig(level=logging.INFO)

Session = namedtuple('Session', ['usrhash', 'country', 'pageviews', 'usertype'])
Pageview = namedtuple('Pageview', ['dt','proj','title','wd'])
EDIT_STR = "EDITATTEMPT"
usertypes = ['reader', 'editor']

def tsv_to_sessions(tsv, trim=False):
    """Convert TSV file of pageviews to reader sessions.

    Each line corresponds to a pageview and the file is sorted by user and then time.
    Fields order is: hashed user ID, wikipedia project, page title, page ID, datettime, IP country, Wikidata ID
    For example:
    00000a5795ba512...  enwiki  Columbidae  63355   2019-02-16T11:31:53 Norway  Q10856
    00000a5795ba512...  enwiki  Anarchism   12      2019-02-16T11:32:05 Norway  Q6199

    This yields a Session object where:
    session.usrhash = '00000a5795ba512...'
    session.country = 'Norway'
    session.pageviews = [(dt='2019-02-16T11:31:53', proj='enwiki', title='Columbidae', wd='Q10856'),
                         (dt='2019-02-16T11:32:05', proj='enwiki', title='Anarchism', wd='Q6199')]
    """
    expected_header = ['user', 'project', 'page_title', 'page_id', 'dt', 'country', 'item_id']
    usr_idx = expected_header.index('user')
    proj_idx = expected_header.index('project')
    title_idx = expected_header.index('page_title')
    dt_idx = expected_header.index('dt')
    country_idx = expected_header.index("country")
    wd_idx = expected_header.index("item_id")
    malformed_lines = 0
    i = 0
    with gzip.open(tsv, 'rt') as fin:
        assert next(fin).strip().split("\t") == expected_header
        curr_usr = None
        country = None
        usertype = 'reader'
        session = []
        for i, line in enumerate(fin):
            line = line.strip().split("\t")
            try:
                usr = line[usr_idx]
                proj = line[proj_idx]
                title = line[title_idx]
                dt = line[dt_idx]
            except IndexError:
                malformed_lines += 1
                continue
            try:
                wd_item = line[wd_idx]
            except IndexError:
                wd_item = None
            pv = Pageview(dt, proj, title, wd_item)
            if usr == curr_usr:
                if title == EDIT_STR:
                    usertype = 'editor'
                else:
                    session.append(pv)
            else:
                if curr_usr:
                    if trim:
                        trim_session(session)
                    yield(Session(curr_usr, country, session, usertype=usertype))
                curr_usr = usr
                country = line[country_idx]
                if title == EDIT_STR:
                    usertype = 'editor'
                    session = []
                else:
                    usertype = 'reader'
                    session = [pv]
        if curr_usr:
            if trim:
                trim_session(session)
            yield (Session(curr_usr, country, session, usertype=usertype))
    print("{0} total lines. {1} malformed.".format(i, malformed_lines))

def trim_session(pvs):
    """Remove duplicate page views (matching title and project).

    For a given session, this retains only the first view of a given page title on a given project.
    Parameters:
        pvs: list of page view objects for a given reader's session
    Returns:
        Nothing. The page views are modified in place.
    """
    # only report based on first pageview of page
    user_unique_pvs = set()
    pvs_to_remove = []
    for i in range(0, len(pvs)):
        pv_id = '{0}-{1}'.format(pvs[i].proj, pvs[i].title)
        if pv_id in user_unique_pvs:
            pvs_to_remove.append(i)
        user_unique_pvs.add(pv_id)
    for i in range(len(pvs_to_remove) - 1, -1, -1):
        pvs.pop(pvs_to_remove[i])

def get_lang_switch(pvs, wikidbs=()):
    """Get pairs of page views that are language switches.

    Parameters:
        pvs: list of page view objects for a given reader's session
        wikidbs: if empty, all language switches return. Otherwise, only language switches that involve languages
                    included in wikidbs will be retained.
    Returns:
        switches: list of tuples, where each tuple corresponds to two page views of a single Wikidata item
                    across two different projects.
                    If a session is:
                    [(dt='2019-02-16T11:31:53', proj='enwiki', title='Columbidae', wd='Q10856'),
                     (dt='2019-02-16T11:32:05', proj='enwiki', title='Anarchism', wd='Q6199'),
                     (dt='2019-02-16T11:32:13', proj='eswiki', title='Columbidae', wd='Q10856')]
                    Then the switches would be of the form [(0, 2)]
    """
    switches = []
    # at least two different projects viewed in the session
    if len(set([p.proj for p in pvs])) > 1:
        # find all wikidata items viewed in multiple languages
        # preserve which one was viewed first
        for i in range(0, len(pvs) - 1):
            for j in range(i+1, len(pvs)):
                diff_proj = pvs[i].proj != pvs[j].proj
                same_item = pvs[i].wd and pvs[i].wd == pvs[j].wd
                if diff_proj and same_item:
                    if not wikidbs or pvs[i].proj in wikidbs or pvs[j].proj in wikidbs:
                        switches.append((i, j))
                        break
    return switches

def get_nonlang_switch(pvs, wikidb, switches=(), direction="from"):
    """Get page views in a language that are not switches of the specified direction.

    Finds pages in a language that the user did not switch to/from (depending on direction parameter).
    User must have at least one language switch with specified wikidb and direction in their session though
    to indicate that they might have switched.

    Parameters:
        pvs: list of page view objects for a given reader's session
        wikidb: Only language non-switches that involve this language will be retained.
        switches: if precalculated, this speeds up processing
        direction: "from" indicates the language switch must have had wikidb as the origin project.
                   "to" indicates the language switch must have had wikidb as the destination project.
    Returns:
        no_switches: list of page view indices.
                For this session and wikidb = "enwiki" and direction = "from":
                [(dt=2019-02-16T11:31:53, proj=enwiki, title='Columbidae', wd='Q10856'),
                 (dt=2019-02-16T11:32:05, proj=enwiki, title='Anarchism', wd='Q6199'),
                 (dt=2019-02-16T11:32:13, proj=eswiki, title='Columbidae', wd='Q10856')]
                Then the no_switches would be of the form: [1]
                If direction was "to" or wikidb was "eswiki" then no page views would be returned.
    """
    no_switches = []
    # at least two different projects viewed in the session
    if len(set([p.proj for p in pvs])) > 1:
        if switches:
            all_switches = switches
        else:
            all_switches = get_lang_switch(pvs, [wikidb])
        # did user have any switches of form:
        # direction == "from": wikidb -> other language
        # direction == "to": other language -> wikidb
        dir_switches_in_lang = set()
        for f,t in all_switches:
            # switched from wikidb -> other project
            if direction == "from" and pvs[f].proj == wikidb:
                dir_switches_in_lang.add(f)
            # switched from other project -> wikidb
            elif direction == "to" and pvs[t].proj == wikidb:
                dir_switches_in_lang.add(t)

        if dir_switches_in_lang:
            # find all wikidata items not viewed in multiple languages
            # preserve which one was viewed first
            for i in range(0, len(pvs)):
                if pvs[i].proj == wikidb and i not in dir_switches_in_lang:
                    no_switches.append(i)
    return no_switches
