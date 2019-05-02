import argparse
import csv
import gzip
import json
import os
import time
import traceback

import mwapi
import pandas as pd

"""
Steps:
 1) Get mapping of all QIDs -> titles in English Wikipedia
 2) Loop through language switches dataset, gathering all the QIDs and their associated English article titles (where possible)
 3) Get the most recent revision ID on English Wikipedia for these QIDs
 4) In a later step, query ORES for drafttopic, which can then be applied to that QID
"""

def get_qid_to_enwikititle():
    qid_to_entitle = {}
    with gzip.open('resources/qid_to_pid.tsv.gz', 'rt') as fin:
        expected_header = ['item_id', 'wiki_db', 'page_id', 'page_title']
        item_idx = expected_header.index('item_id')
        wiki_idx = expected_header.index('wiki_db')
        title_idx = expected_header.index('page_title')
        assert next(fin).strip().split('\t') == expected_header
        for line in fin:
            line = line.strip().split('\t')
            if line[wiki_idx] == "enwiki":
                qid_to_entitle[line[item_idx]] = line[title_idx]
    return qid_to_entitle


def add_revids(langswitches_tsv, output_fn, include_nonswitches=False):
    """Use this to generate the input for the ORES API"""
    header = ['switch', 'country', 'qid', 'title', 'datetime', 'usertype']
    qid_idx = header.index('qid')
    title_idx = header.index('title')
    switch_idx = header.index('switch')
    qid_to_revid = {}
    qid_to_entitle = get_qid_to_enwikititle()
    if os.path.exists(output_fn):
        with open(output_fn, 'r') as fin:
            for line in fin:
                record = json.loads(line)
                try:
                    qid_to_revid[record['qid']] = int(record['rev_id'])
                except TypeError:
                    qid_to_revid[record['qid']] = 0

    max_titles_per_query = 50
    session = mwapi.Session(host='https://en.wikipedia.org',
                            user_agent='mwapi (python) -- m:Research:Language_switching_behavior_on_Wikipedia')
    base_parameters = {'action': 'query',
                       'prop': 'revisions',
                       'format': 'json',
                       'formatversion': '2',
                       'rvprop': 'ids',
                       'rvslots': 'main',
                       'redirects':'true'}

    with open(langswitches_tsv, 'r') as fin:
        tsvreader = csv.reader(fin, delimiter='\t')
        title_to_qid = {}
        i = 0
        for line in tsvreader:
            if i % 1000 == 0:
                print("{0} lines processed.\t{1} revIDs.".format(i, len(qid_to_revid) + len(title_to_qid)))
            i += 1
            if not include_nonswitches and line[switch_idx] == 'N\A' or line[switch_idx] == 'N/A':
                continue
            qid = line[qid_idx]
            if qid and qid not in qid_to_revid:
                # get canonical title from wikidata mapping, else title reported in dataset
                title = qid_to_entitle.get(qid, line[title_idx])
                if title and title not in title_to_qid:
                    title_to_qid[title] = qid
                    if len(title_to_qid) == max_titles_per_query:
                        try:
                            title_to_revid = get_revids_by_title(session, base_parameters, title_to_qid)
                            qid_to_revid.update({title_to_qid[title]:revid for title,revid in title_to_revid.items()})
                            title_to_qid = {}
                        except Exception:
                            traceback.print_exc()
                            print("Breaking off at line {0}".format(i+1))
                            title_to_qid = {}
                            break
        title_to_revid = get_revids_by_title(session, base_parameters, title_to_qid)
        qid_to_revid.update({title_to_qid[title]:revid for title, revid in title_to_revid.items()})
        print("Finished: {0} lines processed. {1} revIDs.".format(i, len(qid_to_revid)))

    # dump in correct format
    revid_df = pd.DataFrame([(qid, revid) for qid, revid in qid_to_revid.items()],
                            columns=['qid', 'rev_id'])
    revid_df['rev_id'] = revid_df['rev_id'].astype('int64')
    revid_df.to_json(path_or_buf=output_fn, orient='records', lines=True)


def get_revids_by_title(session, base_parameters, titles):
    title_to_revid = {}
    if titles:
        time.sleep(1)
        params = base_parameters.copy()
        params['titles'] = '|'.join(titles)
        mostrecent_revids = session.get(params)
        title_map = {t:t for t in titles}
        for denormed in mostrecent_revids['query'].get('normalized', {}):
            title_map[denormed['to']] = denormed['from']
        for redirected in mostrecent_revids['query'].get('redirects', {}):
            title_map[redirected['to']] = title_map[redirected['from']]
        for page in mostrecent_revids['query']['pages']:
            query_title = title_map[page['title']]
            try:
                title_to_revid[query_title] = int(page['revisions'][0]['revid'])
            except KeyError:
                print("Skipping: {0}\t{1}".format(query_title, page['title']))
                title_to_revid[query_title] = 0
    return title_to_revid

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--switches_tsvs", nargs="+")
    parser.add_argument("--include_nonswitches", action="store_true", default=False)
    args = parser.parse_args()

    for fn in args.switches_tsvs:
        if not os.path.exists(fn):
            print("{0} does not exist. Skipping.".format(fn))
        else:
            dir = os.path.dirname(fn)
            lang = os.path.basename(fn).split('_')[0]
            output_fn = os.path.join(dir, 'qid_revids.json')
            print("Processing {0}. From {1} to {2}".format(lang, fn, output_fn))
            time.sleep(3)
            add_revids(fn, output_fn, args.include_nonswitches)


