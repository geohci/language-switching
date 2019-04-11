import argparse
from copy import deepcopy
import csv
import glob
import logging

import pandas as pd

from session_utils import tsv_to_sessions
from session_utils import get_lang_switch
from session_utils import usertypes

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tsvs", nargs="+",
                        help=".tsv files with anonymized page views ordered by user/datetime")
    parser.add_argument("--stopafter", type=int, default=-1,
                        help="Process only this many sessions.")
    parser.add_argument("--debug", action="store_true",
                        help="More verbose logging")
    parser.add_argument("--maxpvs", type=int, default=500,
                        help="Max pageviews in a session to still be included in analysis.")
    parser.add_argument("--switch_fn", default="switches_by_proj.tsv")
    parser.add_argument("--cooc_fn", default="cooc_by_proj.tsv")
    args = parser.parse_args()

    if len(args.tsvs) == 1:
        args.tsvs = glob.glob(args.tsvs[0])
    logging.info(args)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.info(("Filtering of statistics:\n"
                  "\t*only Wikipedia projects considered (.wikipedia; mobile/desktop/app aggregated)\n"
                  "\t*only namespace = 0 considered\n"
                  "\t*only first pageview of a given article retained (e.g. enwiki+Chicago)\n"
                  "\t*language switching defined as same wikidata item, different project\n"
                  "\t*devices w/ greater than {0} pageviews dropped as likely bots.".format(args.maxpvs)))

    # count of language pairs involved in switches (directional)
    switch_to_from = {}
    # count of languages co-occurring in same session (whether switch or not)
    lang_cooccurrence = {}
    # number of unique language projects per session
    lang_counts = {}
    # number of views per language project
    proj_pvs = {}

    for d in [switch_to_from, lang_cooccurrence, lang_counts, proj_pvs]:
        for ut in usertypes:
            d[ut] = {}

    i = 0
    for tsv in args.tsvs:
        logging.info("Processing: {0}".format(tsv))
        # this only includes the first page view for a given QID-project so a user repeatedly viewing a page doesn't skew the statistics
        for session in tsv_to_sessions(tsv, trim=True):
            if i == args.stopafter:
                break
            i += 1
            if i % 500000 == 0:
                logging.info("{0} sessions analyzed.".format(i))

            ut = session.usertype

            # filter out likely bots
            pvs = session.pageviews
            num_pvs = len(pvs)
            if num_pvs > args.maxpvs:
                continue

            unique_langs = set([p.proj for p in pvs])
            for proj in unique_langs:
                proj_pvs[ut][proj] = proj_pvs[ut].get(proj, 0) + 1

            num_langs = len(unique_langs)
            lang_counts[ut][num_langs] = lang_counts[ut].get(num_langs, 0) + 1
            if num_langs > 1:
                lang_switches = get_lang_switch(pvs)
                tfs = set()
                for ls_pair in lang_switches:
                    frompv = pvs[ls_pair[0]]
                    topv = pvs[ls_pair[1]]
                    tf = '{0}-{1}'.format(frompv.proj, topv.proj)
                    tfs.add(tf)
                for tf in tfs:
                    switch_to_from[ut][tf] = switch_to_from[ut].get(tf, 0) + 1
                sorted_langs = sorted(unique_langs)
                for li in range(0, num_langs - 1):
                    for lj in range(li+1, num_langs):
                        tf = '{0}-{1}'.format(sorted_langs[li], sorted_langs[lj])
                        lang_cooccurrence[ut][tf] = lang_cooccurrence[ut].get(tf, 0) + 1
            else:
                single_lang = pvs[0].proj
                tf = '{0}-{0}'.format(single_lang)
                lang_cooccurrence[ut][tf] = lang_cooccurrence[ut].get(tf, 0) + 1
                switch_to_from[ut][tf] = switch_to_from[ut].get(tf, 0) + 1


    logging.info("\nLangs per userhash:")
    for ut in usertypes:
        logging.info("==={0}===".format(ut))
        print_stats(lang_counts[ut], 10, "langs")

    logging.info("\nLanguage pairs:")
    for ut in usertypes:
        logging.info("==={0}===".format(ut))
        print_stats(switch_to_from[ut], 30, "")

    logging.info("\nWeighted language pairs:")
    for ut in usertypes:
        logging.info("==={0}===".format(ut))
        weighted_to_from = weight_by_proj(switch_to_from[ut], proj_pvs[ut])
        print_stats(weighted_to_from, 20, "", context_dict=switch_to_from[ut])

    if args.switch_fn:
        for ut in usertypes:
            with open(args.switch_fn.replace('.tsv', '_{0}.tsv'.format(ut)), "w") as fout:
                csvwriter = csv.writer(fout, delimiter="\t")
                csvwriter.writerow(['to', 'from', 'count', 'to_lang_totalsessions', 'from_lang_totalsessions'])
                for tf in switch_to_from[ut]:
                    tolang, fromlang = tf.split("-")
                    count = switch_to_from[ut].get(tf)
                    csvwriter.writerow([tolang, fromlang, count, proj_pvs[ut][tolang], proj_pvs[ut][fromlang]])
#        lang_coocurrence_csv(switch_to_from, proj_pvs)

    if args.cooc_fn:
        for ut in usertypes:
            with open(args.cooc_fn.replace('.tsv', '_{0}.tsv'.format(ut)), "w") as fout:
                csvwriter = csv.writer(fout, delimiter="\t")
                csvwriter.writerow(['to', 'from', 'count', 'to_lang_totalpv', 'from_lang_totalpv'])
                for lc in lang_cooccurrence[ut]:
                    l1, l2 = lc.split("-")
                    count = lang_cooccurrence[ut].get(lc)
                    csvwriter.writerow([l1, l2, count, proj_pvs[ut][l1], proj_pvs[ut][l2]])
                    if l1 != l2:
                        csvwriter.writerow([l2, l1, count, proj_pvs[ut][l2], proj_pvs[ut][l1]])
#       lang_coocurrence_csv(lang_cooccurrence, proj_pvs)


def lang_coocurrence_csv(switches, lang_counts, fn=None):
    lang_sorted_by_popularity = sorted(lang_counts, key=lang_counts.get, reverse=True)
    lang_overlap = pd.DataFrame(index=lang_sorted_by_popularity, columns=lang_sorted_by_popularity, dtype="float32")
    for l_to in lang_sorted_by_popularity:
        for l_from in lang_sorted_by_popularity:
            to_from = switches.get('{0}-{1}'.format(l_to, l_from), 0)
            lang_overlap.loc[l_to, l_from] = to_from
            from_to = switches.get('{0}-{1}'.format(l_from, l_to), 0)
            lang_overlap.loc[l_from, l_to] = from_to
    if fn:
        lang_overlap.to_csv(fn, sep="\t")
    else:
        print(lang_overlap)

def weight_by_proj(countdict, pvs_by_proj, minpv_threshold=500):
    """Normalize count stats by how many page views occurred on a project"""
    # have to deep copy otherwise, this will change in-place and affect other statistics
    countdict = deepcopy(countdict)
    for pi_to_pj in list(countdict.keys()):
        count = countdict.pop(pi_to_pj)
        pi = pi_to_pj.split("-")[0]
        norm = pvs_by_proj[pi]
        # only compute proportion for projects w/ enough traffic that the pattern MIGHT be real
        if norm > minpv_threshold:
            countdict[pi_to_pj] = count / norm
        else:
            countdict[pi_to_pj] = 0
    return countdict


def weight_by_pvs(countdict, pvs_by_wd, minpv_threshold=50):
    """Normalize count stats by how often a page was viewed"""
    for wd_title in list(countdict.keys()):
        count = countdict.pop(wd_title)
        norm = pvs_by_wd[wd_title]
        # only compute proportion for pages w/ enough traffic that the pattern MIGHT be real
        if norm > minpv_threshold:
            countdict[wd_title] = count / norm
        else:
            countdict[wd_title] = 0

def print_stats(countdict, threshold, lbl, ignore=(), context_dict=None):
    """Print limited statistics for count dictionaries."""
    rest = 0
    ignorevals = []
    for k in ignore:
        ignorevals.append((k, countdict.pop(k)))
    denominator = sum(countdict.values())
    for topk, sc in enumerate([(k, countdict[k]) for k in sorted(countdict, key=countdict.get, reverse=True)]):
        if topk <= threshold:
            if context_dict:
                logging.info("{0} {1}:\t{2}\t({3}) times.".format(sc[0], lbl, sc[1], context_dict.get(sc[0], "N/A")))
            else:
                logging.info("{0} {1}:\t{2}\t({3:.3f}) times.".format(sc[0], lbl, sc[1], sc[1] / denominator))
        else:
            rest += sc[1]
    if rest:
        logging.info("Remainder:\t{0}\t({1:.3f}) times.".format(rest, rest / denominator))
    for k,v in ignorevals:
        countdict[k] = v




if __name__ == "__main__":
    main()