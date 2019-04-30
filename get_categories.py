import argparse
import csv
import os
import time

import mwapi
import pandas as pd

def add_revids(lang, langswitches_tsv, output_fn):
    """Use this to generate the input for the ORES API"""
    header = ['switch', 'country', 'qid', 'title', 'datetime', 'usertype']
    title_idx = header.index('title')
    qid_idx = header.index('qid')
    qid_to_revid = {}
    max_titles_per_query = 50
    session = mwapi.Session(host='https://{0}.wikipedia.org'.format(lang),
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
        pages_to_query = []
        title_to_qid = {}
        i = 0
        for line in tsvreader:
            pagetitle = line[title_idx]
            qid = line[qid_idx]
            title_to_qid[pagetitle] = qid
            if qid not in qid_to_revid and pagetitle not in pages_to_query:
                pages_to_query.append(pagetitle)
                if len(pages_to_query) == max_titles_per_query:
                    title_to_revid = get_revids_by_title(session, base_parameters, pages_to_query)
                    qid_to_revid.update({title_to_qid[title]:revid for title,revid in title_to_revid.items()})
                    pages_to_query = []
            i += 1
            if i % 10000 == 0:
                print("{0} lines processed.\t{1} revIDs.".format(i, len(qid_to_revid) + len(pages_to_query)))
        title_to_revid = get_revids_by_title(session, base_parameters, pages_to_query)
        qid_to_revid.update({title_to_qid[title]:revid for title,revid in title_to_revid.items()})
        print("Finished: {0} lines processed. {1} revIDs.".format(i, len(qid_to_revid)))

    # dump in correct format
    revid_df = pd.DataFrame([(qid, revid) for qid, revid in qid_to_revid.items()],
                            columns=['qid', 'revid'])
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
            title_to_revid[title_map[page['title']]] = page['revisions'][0]['revid']
    return title_to_revid

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--switches_tsvs", nargs="+")
    args = parser.parse_args()

    for fn in args.switches_tsvs:
        if not os.path.exists(fn):
            print("{0} does not exist. Skipping.".format(fn))
        else:
            dir = os.path.dirname(fn)
            lang = os.path.basename(fn).split('_')[0]
            output_fn = os.path.join(dir, '{0}_revids.json'.format(lang))
            print("Processing {0}. From {1} to {2}".format(lang, fn, output_fn))
            time.sleep(3)
            add_revids(lang, fn, output_fn)


