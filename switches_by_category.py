import argparse
import json

import pandas as pd

def get_pred_topic(input_json):
    try:
        topic = input_json['score']['drafttopic']['score']['prediction'][0]
    except (IndexError, KeyError) as error:
        topic = None
    return topic

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ores_output")
    parser.add_argument("--switches_tsv")
    args = parser.parse_args()

    qid_to_topic = {}
    with open(args.ores_output, 'r') as fin:
        for line in fin:
            record = json.loads(line)
            qid = record['qid']
            topic = get_pred_topic(record)
            qid_to_topic[qid] = topic

    no_topic = 0
    non_switch = 0
    switch_topics = {}
    with open(args.switches_tsv, 'r') as fin:
        header = ['switch', 'country', 'qid', 'title', 'datetime', 'usertype']
        switch_idx = header.index('switch')
        qid_idx = header.index('qid')
        for line in fin:
            line = line.strip().split('\t')
            if line[switch_idx] == 'N\A' or line[switch_idx] == 'N/A':
                non_switch += 1
                continue
            qid = line[qid_idx]
            try:
                topic = qid_to_topic[qid]
            except KeyError:
                no_topic += 1
                continue
            switch_topics[topic] = switch_topics.get(topic, 0) + 1
    total_switches = sum(switch_topics.values())
    print("{0} different topics, {1} switches w/ topics. {2} switches w/o topics. {3} non-switches.".format(
        len(switch_topics), total_switches, no_topic, non_switch))

    topicdf = pd.DataFrame([(topic, count) for topic, count in switch_topics.items()], columns=['topic', 'count'])
    topicdf['proportion'] = topicdf['count'].apply(lambda x: x / total_switches)

    topicdf.sort_values(by='proportion', ascending=False, inplace=True)
    print(topicdf)

    toptopics = {}
    for t in switch_topics:
        if t:
            toptopic = t.split('.')[0]
        else:
            toptopic = 'None'
        toptopics[toptopic] = toptopics.get(toptopic, 0) + switch_topics[t]
    toptopicdf = pd.DataFrame([(topic, count) for topic, count in toptopics.items()], columns=['topic', 'count'])
    toptopicdf['proportion'] = toptopicdf['count'].apply(lambda x: x / total_switches)

    toptopicdf.sort_values(by='proportion', ascending=False, inplace=True)
    print(toptopicdf)

if __name__ == "__main__":
    main()