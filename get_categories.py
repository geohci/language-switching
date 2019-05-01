import argparse
import csv
import json
import os
import time
import traceback

import mwapi
import pandas as pd

def add_revids(lang, langswitches_tsv, output_fn):
    """Use this to generate the input for the ORES API"""
    header = ['switch', 'country', 'qid', 'title', 'datetime', 'usertype']
    title_idx = header.index('title')
    qid_idx = header.index('qid')
    qid_to_revid = {}
    if os.path.exists(output_fn):
        with open(output_fn, 'r') as fin:
            for line in fin:
                record = json.loads(line)
                qid_to_revid[record['qid']] = int(record['rev_id'])

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
                    try:
                        title_to_revid = get_revids_by_title(session, base_parameters, pages_to_query)
                        qid_to_revid.update({title_to_qid[title]:revid for title,revid in title_to_revid.items()})
                        pages_to_query = []
                    except Exception:
                        traceback.print_exc()
                        print("Breaking off at line {0}".format(i+1))
                        pages_to_query = []
                        break
            i += 1
            if i % 1000 == 0:
                print("{0} lines processed.\t{1} revIDs.".format(i, len(qid_to_revid) + len(pages_to_query)))
        title_to_revid = get_revids_by_title(session, base_parameters, pages_to_query)
        qid_to_revid.update({title_to_qid[title]:revid for title,revid in title_to_revid.items()})
        print("Finished: {0} lines processed. {1} revIDs.".format(i, len(qid_to_revid)))

    # dump in correct format
    revid_df = pd.DataFrame([(qid, revid) for qid, revid in qid_to_revid.items()],
                            columns=['qid', 'revid'])
    revid_df['revid'] = revid_df['revid'].astype('int64')
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
            dataset_title = title_map[page['title']]
            try:
                title_to_revid[dataset_title] = int(page['revisions'][0]['revid'])
            except KeyError:
                print("Skipping: {0}\t{1}".format(dataset_title, page['title']))
                title_to_revid[dataset_title] = None
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
            output_fn = os.path.join(fn.replace(".tsv", "_revid.json"))
            if output_fn == fn:
                print("Skipping. Expected TSV:", output_fn)
                continue
            print("Processing {0}. From {1} to {2}".format(lang, fn, output_fn))
            time.sleep(3)
            add_revids(lang, fn, output_fn)


