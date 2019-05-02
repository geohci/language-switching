import argparse
import json

import numpy as np
import pandas as pd

NON_SWITCHES = ('N\A', 'N/A')

def get_pred_topic_naive(input_json):
    try:
        topic = input_json['score']['drafttopic']['score']['prediction'][0]
    except (IndexError, KeyError) as error:
        topic = None
    return topic

def get_pred_topic_rand(input_json):
    try:
        topic = np.random.choice(input_json['score']['drafttopic']['score']['prediction'])
    except (KeyError, ValueError) as error:
        topic = None
    return topic

def get_pred_topic_all(input_json):
    try:
        topics = input_json['score']['drafttopic']['score']['prediction']
    except (IndexError, KeyError) as error:
        topics = []
    return topics

def get_pred_topic_best(input_json):
    try:
        topics = input_json['score']['drafttopic']['score']['probability']
        best = sorted(topics, key=topics.get, reverse=True)[0]
    except (IndexError, KeyError) as error:
        best = None
    return best

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ores_output")
    parser.add_argument("--switches_tsv")
    parser.add_argument("--approach", default='best', help="How to count topics: one of naive, rand, all, best.")
    args = parser.parse_args()

    approaches = {'naive': get_pred_topic_naive,
                  'rand': get_pred_topic_rand,
                  'all': get_pred_topic_all,
                  'best': get_pred_topic_best}
    get_pred_topic = approaches[args.approach]

    print("==== {0} ====".format(args.approach))
    qid_to_topic = {}
    with open(args.ores_output, 'r') as fin:
        for line in fin:
            record = json.loads(line)
            qid = record['qid']
            topic = get_pred_topic(record)
            qid_to_topic[qid] = topic

    s_no_topic = 0
    n_no_topic = 0
    switch_topics = {}
    noswitch_topics = {}
    with open(args.switches_tsv, 'r') as fin:
        header = ['switch', 'country', 'qid', 'title', 'datetime', 'usertype']
        switch_idx = header.index('switch')
        qid_idx = header.index('qid')
        for line in fin:
            line = line.strip().split('\t')
            qid = line[qid_idx]
            try:
                topic = qid_to_topic[qid]
            except KeyError:
                if line[switch_idx] in NON_SWITCHES:
                    n_no_topic += 1
                else:
                    s_no_topic += 1
                continue
            if args.approach == 'all':
                for t in topic:
                    if line[switch_idx] in NON_SWITCHES:
                        noswitch_topics[t] = noswitch_topics.get(t, 0) + 1
                    else:
                        switch_topics[t] = switch_topics.get(t, 0) + 1
            else:
                if line[switch_idx] in NON_SWITCHES:
                    noswitch_topics[topic] = noswitch_topics.get(topic, 0) + 1
                else:
                    switch_topics[topic] = switch_topics.get(topic, 0) + 1
    total_switches = sum(switch_topics.values())
    total_nonswitches = sum(noswitch_topics.values())
    print("    Switches:\t{0} different topics;\t{1} w/ topics;\t{2} w/o topics.".format(
        len(switch_topics), total_switches, s_no_topic))
    print("Non-switches:\t{0} different topics;\t{1} w/ topics;\t{2} w/o topics.".format(
        len(noswitch_topics), total_nonswitches, n_no_topic))

    all_topics = set(switch_topics.keys())
    all_topics.update(noswitch_topics.keys())
    topicdf = pd.DataFrame([
        (topic, switch_topics.get(topic, 0), noswitch_topics.get(topic, 0)) for topic in all_topics],
        columns=['topic', 'switch_count', 'nonswitch_count'])
    topicdf['switch_proportion'] = topicdf['switch_count'].apply(lambda x: x / total_switches)
    topicdf['nonswitch_proportion'] = topicdf['nonswitch_count'].apply(lambda x: x / total_nonswitches)

    topicdf.sort_values(by='switch_proportion', ascending=False, inplace=True)
    print(topicdf)

    s_toptopics = {}
    n_toptopics = {}
    for t in all_topics:
        if t:
            toptopic = t.split('.')[0]
        else:
            toptopic = 'None'
        s_toptopics[toptopic] = s_toptopics.get(toptopic, 0) + switch_topics.get(t, 0)
        n_toptopics[toptopic] = n_toptopics.get(toptopic, 0) + noswitch_topics.get(t, 0)
    toptopicdf = pd.DataFrame([
        (topic, s_toptopics[topic], n_toptopics[topic]) for topic in s_toptopics],
        columns=['topic', 'switch_count', 'nonswitch_count'])
    toptopicdf['switch_proportion'] = toptopicdf['switch_count'].apply(lambda x: x / total_switches)
    toptopicdf['nonswitch_proportion'] = toptopicdf['nonswitch_count'].apply(lambda x: x / total_nonswitches)

    toptopicdf.sort_values(by='switch_proportion', ascending=False, inplace=True)
    print(toptopicdf)

if __name__ == "__main__":
    main()