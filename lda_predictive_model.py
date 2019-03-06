import argparse
import csv
import logging
import os
import pickle

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score

from session_utils import tsv_to_sessions
from session_utils import get_lang_switch
from session_utils import get_nonlang_switch

def load_topic_model(lda_dir, lang):
    # load in topic model for selected language
    with open(os.path.join(lda_dir, '{0}_lda_features.csv'.format(lang)), 'r') as fin:
        csvreader = csv.reader(fin, delimiter="\t")
        example = next(csvreader)
        # title, feature vector
        ndims = len(example) - 1
    logging.debug("{0} dimensions for LDA topic model".format(ndims))

    with open(os.path.join(lda_dir, '{0}_titles.p'.format(lang)), 'rb') as fin:
        tlist = pickle.load(fin)
    titles = {t.replace(" ", "_"):i for i,t in enumerate(tlist)}
    logging.debug("{0} titles in LDA topic model".format(len(titles)))
    topic_model = np.zeros(shape=(len(titles), ndims), dtype=np.float32)
    with open(os.path.join(lda_dir, '{0}_lda_features.csv'.format(lang)), 'r') as fin:
        for i, line in enumerate(fin):
            line = line.strip().split("\t")
            article_title = line[0].replace(" ", "_")
            embedding = [float(d) for d in line[1:]]
            if i != titles.get(article_title, -1):
                logging.debug("Misaligned:\t{0}\t{1}\t{2}".format(article_title, i, titles.get(article_title, -1)))
                raise ValueError("LDA title list did not match with topic vectors.")
            topic_model[i] = embedding

    wordline_prefix = 'Top words: '
    prefix_len = len(wordline_prefix)
    topic_descs = []
    with open(os.path.join(lda_dir, '{0}_overview.txt'.format(lang)), 'r') as fin:
        for line in fin:
            if line.startswith(wordline_prefix):
                topic_descs.append(line[prefix_len:].strip())

    logging.debug('{0} topics; example: {1}'.format(len(topic_descs), topic_descs[0]))

    return (ndims, titles, topic_model, topic_descs)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tsvs", nargs="+",
                        help=".tsv files with anonymized page views ordered by user/datetime")
    parser.add_argument("--lda_dir", default="/home/flemmerich/wikimotifs2/data/text/",
                        help="directory holding LDA topic models and metadata")
    parser.add_argument("--lang", default="eswiki",
                        help="Language to build dataset for -- e.g., eswiki")
    parser.add_argument("--direction", default="from",
                        help="Either to or from depending on if switch should be from lang or to lang")
    parser.add_argument("--stopafter", type=int, default=-1,
                        help="Process only this many sessions.")
    parser.add_argument("--debug", action="store_true",
                        help="More verbose logging")
    parser.add_argument("--maxpvs", type=int, default=500,
                        help="Max pageviews in a session to still be included in analysis.")
    parser.add_argument("--numfolds", type=int, default=10,
                        help="number of folds for new train/test of logistic regression model")
    parser.add_argument("--holdout_type", default="random", help="")
    parser.add_argument("--output_tsv", default=None,
                        help=".tsv file to write balanced dataset to for future analyses")
    parser.add_argument("--results_tsv", default=None,
                        help=".tsv file to write model results to")
    args = parser.parse_args()

    logging.info(("Assumptions:\n"
                   "\t*only Wikipedia projects considered (.wikipedia; mobile/desktop/app aggregated)\n"
                   "\t*language switching defined as same wikidata item, different project\n"
                   "\t*non-language switches are pages by users who did switch, but not on that article (thus suggesting that the user knows how to switch but chose not to)\n"
                   "\t*devices w/ greater than {0} pageviews dropped as likely bots.".format(args.maxpvs)))


    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.info("Args: {0}".format(args))
    wiki_db = args.lang
    direction = args.direction
    wiki_lang = wiki_db.replace("wiki", "")
    if wiki_db == wiki_lang:
        raise Exception("Invalid lang. Should be like enwiki: {0}".format(args.lang_from))
    if direction not in ("to", "from"):
        raise Exception("Invalid direction. Should be either 'to' or 'from'")

    ndims, titles, topic_model, topic_descs = load_topic_model(args.lda_dir, wiki_lang)

    switches = []
    non_switches = []
    if args.output_tsv and os.path.exists(args.output_tsv):
        logging.info("Loading data from: {0}".format(args.output_tsv))
        with open(args.output_tsv, 'r') as fin:
            tsvreader = csv.reader(fin, delimiter="\t")
            for line in tsvreader:
                if line[0] == 'S':
                    switches.append(line[1:])
                elif line[0] == 'N':
                    non_switches.append(line[1:])
    else:
        logging.info("Building balanced dataset of switches / non-switches")
        wd_to_entitle = {}
        # build balanced dataset
        i = 0
        for tsv in args.tsvs:
            logging.info("Analyzing: {0}".format(tsv))
            for session in tsv_to_sessions(tsv, trim=True):
                if i == args.stopafter:
                    break
                i += 1
                if i % 500000 == 0:
                    logging.info("{0} sessions analyzed.".format(i))

                # filter out likely bots
                num_pvs = len(session.pageviews)
                if num_pvs > args.maxpvs:
                    continue

                # QID -> English title for more interpretable results
                for pv in session.pageviews:
                    if pv.wd and pv.proj == 'enwiki':
                        wd_to_entitle[pv.wd] = pv.title

                # only analyze language switching when >1 pageview associated w/ device (~50% of sessions)
                if num_pvs > 1:
                    pvs = session.pageviews
                    unique_langs = set([p.proj for p in pvs])
                    # has language of interest and at least one potential switch
                    candidate = wiki_db in unique_langs and len(unique_langs) > 1
                    if candidate:
                        user_switches = get_lang_switch(pvs, [wiki_db])
                        # only include users with switches (even if they don't match the direction)
                        if user_switches:
                            user_non_switches = get_nonlang_switch(pvs, wiki_db, user_switches, direction=direction)
                            if direction == "from":
                                switches.extend([(session.country, pvs[i].wd, pvs[i].title) for i,j in user_switches if pvs[i].proj == wiki_db])
                                non_switches.extend([(session.country, pvs[i].wd, pvs[i].title) for i in user_non_switches if pvs[i].proj == wiki_db])
                            elif direction == "to":
                                switches.extend([(session.country, pvs[j].wd, pvs[j].title) for i,j in user_switches if pvs[j].proj == wiki_db])
                                non_switches.extend([(session.country, pvs[i].wd, pvs[i].title) for i in user_non_switches if pvs[i].proj == wiki_db])
                            logging.debug('{0} pvs:\t{1}'.format(len(pvs), pvs))
                            logging.debug('    Switches:\t{0}'.format([(pvs[i], pvs[j]) for i,j in user_switches]))
                            logging.debug('Non-switches:\t{0}'.format([pvs[i] for i in user_non_switches]))

        logging.info("{0} sessions analyzed.".format(i))

        logging.info("Before filtering:")
        logging.info("{0} switches.".format(len(switches)))
        logging.info("{0} non switches.".format(len(non_switches)))

        # make sure we have LDA vectors for the titles
        logging.info("After filtering to only titles with LDA topics:")
        removed = set()
        for idx in range(len(switches)-1, -1, -1):
            if switches[idx][2] not in titles:
                removed.add(switches.pop(idx)[2])
        for idx in range(len(non_switches)-1, -1, -1):
            if non_switches[idx][2] not in titles:
                removed.add(non_switches.pop(idx)[2])
        logging.info("{0} switches.".format(len(switches)))
        logging.info("{0} non switches.".format(len(non_switches)))
        logging.info("{0} removed: {1}".format(len(removed), removed))

        # only keep as many as there are switches so we have a balanced dataset
        logging.info("After balancing:")
        keep_indices = np.random.choice(len(non_switches), len(switches), replace=False)
        non_switches = [non_switches[idx] for idx in keep_indices]

        if args.output_tsv:
            with open(args.output_tsv, 'w') as fout:
                csvwriter = csv.writer(fout, delimiter="\t")
                for s in switches:
                    csvwriter.writerow(['S'] + [f for f in s])
                for n in non_switches:
                    csvwriter.writerow(['N'] + [f for f in n])

    logging.info("{0} switches.".format(len(switches)))
    logging.info("{0} non switches.".format(len(non_switches)))

    X = np.zeros(shape=(len(switches) + len(non_switches), ndims))
    i = 0
    for s in switches:
        X[i] = topic_model[titles[s[2]]]
        i += 1
    for n in non_switches:
        X[i] = topic_model[titles[n[2]]]
        i += 1
    y = [1] * len(switches) + [0] * len(non_switches)

    logging.info("Actual:")
    pred_scores = predictive_model(X, y, num_folds=args.numfolds, topic_descs=topic_descs)
    logging.info("Baseline:")
    baseline_scores = predictive_model(np.random.random(X.shape), y, num_folds=args.numfolds)
    if args.results_tsv:
        with open(args.results_tsv, 'a') as fout:
            tsvwriter = csv.writer(fout, delimiter="\t")
            tsvwriter.writerow([wiki_lang, 'predictions', len(X), pred_scores])
            tsvwriter.writerow([wiki_lang, 'baseline', len(X), baseline_scores])

def predictive_model(X, y, num_folds=5, topic_descs=None):
    clf = LogisticRegression(solver='lbfgs', penalty='l2', C=0.1)
    scores = cross_val_score(estimator=clf, X=X, y=y, cv=num_folds)
    logging.info("Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() * 2))
    if topic_descs:
        clf.fit(X, y)
        coef_importance = np.argsort(clf.coef_[0])
        top_three = coef_importance[-3:][::-1]
        for idx in top_three:
            logging.debug('{0}: {1}'.format(clf.coef_[0][idx], topic_descs[idx]))
        bot_three = coef_importance[:3]
        for idx in bot_three:
            logging.debug('{0}: {1}'.format(clf.coef_[0][idx], topic_descs[idx]))

    return scores

if __name__ == "__main__":
    main()