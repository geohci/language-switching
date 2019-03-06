import argparse
from copy import deepcopy
import logging

from session_utils import tsv_to_sessions
from session_utils import get_lang_switch

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tsvs", nargs="+",
                        help=".tsv files with anonymized page views ordered by user/datetime")
    parser.add_argument("--langs", nargs="+",
                        help="if included, specific languages to only track switching statistics for")
    parser.add_argument("--stopafter", type=int, default=-1,
                        help="Process only this many sessions.")
    parser.add_argument("--debug", action="store_true",
                        help="More verbose logging")
    parser.add_argument("--maxpvs", type=int, default=500,
                        help="Max pageviews in a session to still be included in analysis.")
    parser.add_argument("--language_stats", default="enwiki",
                        help="Print article statistics for this language")
    parser.add_argument("--wdids_to_print", nargs="+", default=[],
                        help="Wikidata IDs to track more thoroughly")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.info(("Filtering of statistics:\n"
                  "\t*only Wikipedia projects considered (.wikipedia; mobile/desktop/app aggregated)\n"
                  "\t*only namespace = 0 considered\n"
                  "\t*only first pageview of a given article retained (e.g. enwiki+Chicago)\n"
                  "\t*language switching defined as same wikidata item, different project\n"
                  "\t*devices w/ greater than {0} pageviews dropped as likely bots.".format(args.maxpvs)))

    # count of language pairs involved in switches (directional)
    to_from = {}
    # number of page views per session
    pv_counts = {}
    # number of unique language projects per session
    lang_counts = {}
    # number of switches per session
    switch_counts = {}
    # specific stats for switches of form: <other language> -> args.language_stats
    lang_to = {}
    # specific stats for switches of form: args.language_stats -> <other language>
    lang_from = {}
    # number of views per wikidata item (across all languages)
    wd_pvs = {}
    # number of views per language project
    proj_pvs = {}
    # map QIDs to English titles (if in dataset) for easier debugging purposes
    wd_to_entitle = {}
    # if wdids_to_print, keep track of the switches for these Wikidata IDs
    wd_examples = {}
    for wditem in args.wdids_to_print:
        wd_examples[wditem] = {}

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

            # filter out likely bots
            num_pvs = len(session.pageviews)
            pv_counts[num_pvs] = pv_counts.get(num_pvs, 0) + 1
            if num_pvs > args.maxpvs:
                continue

            for pv in session.pageviews:
                wditem = pv.wd
                if wditem:
                    wd_pvs[wditem] = wd_pvs.get(wditem, 0) + 1
                    if pv.proj == 'enwiki':
                        wd_to_entitle[wditem] = pv.title
                proj_pvs[pv.proj] = proj_pvs.get(pv.proj, 0) + 1

            # only analyze language switching when >1 pageview associated w/ device (~50% of sessions)
            if num_pvs > 1:
                pvs = session.pageviews
                num_langs = len(set([p.proj for p in pvs]))
                lang_counts[num_langs] = lang_counts.get(num_langs, 0) + 1
                if num_langs > 1:
                    lang_switches = get_lang_switch(pvs)
                    num_switches = len(lang_switches)
                    switch_counts[num_switches] = switch_counts.get(num_switches, 0) + 1
                    for ls_pair in lang_switches:
                        frompv = pvs[ls_pair[0]]
                        topv = pvs[ls_pair[1]]
                        if not args.langs or frompv.proj in args.langs or topv.proj in args.langs:
                            tf = '{0}-{1}'.format(frompv.proj, topv.proj)
                            to_from[tf] = to_from.get(tf, 0) + 1
                            if frompv.wd in args.wdids_to_print:
                                wd_examples[frompv.wd][tf] = wd_examples[frompv.wd].get(tf, 0) + 1

                        if frompv.proj == args.language_stats:
                            lang_from[frompv.wd] = lang_from.get(frompv.wd, 0) + 1
                        elif topv.proj == args.language_stats:
                            lang_to[topv.wd] = lang_to.get(topv.wd, 0) + 1

    logging.info("{0} users with switches ({1} false alarms) out of {2} sessions.".format(
        sum([v for k,v in switch_counts.items() if k > 0]), switch_counts.get(0, -1), i))

    # print summary stats on sessions
    logging.info("\nPVs per userhash:")
    print_stats(pv_counts, 10, "pageviews")

    logging.info("\nLangs per userhash:")
    logging.info("Not included because 1 pageview: {0}".format(pv_counts[1]))
    print_stats(lang_counts, 10, "langs")

    logging.info("\nSwitches per userhash:")
    logging.info("Not included because 1 pageview: {0}".format(pv_counts[1]))
    logging.info("Not included because 2+ pageviews but 1 language: {0}".format(lang_counts[1]))
    print_stats(switch_counts, 10, "switches")

    # print summary stats on individual pages
    # normalize keys w/ wd-item + english title if available for easier interpretation
    for d in [wd_pvs, lang_from, lang_to]:
        for wditem in list(d.keys()):
            entitle = wd_to_entitle.get(wditem, "UNK")
            count = d.pop(wditem)
            d['{0} ({1})'.format(wditem, entitle)] = count

    logging.info("\n{0} pages from:".format(args.language_stats))
    print_stats(lang_from, 20, "")

    logging.info("\nWeighted {0} pages from:".format(args.language_stats))
    weight_by_pvs(lang_from, wd_pvs)
    print_stats(lang_from, 20, "", context_dict=wd_pvs)

    logging.info("\n{0} pages to:".format(args.language_stats))
    print_stats(lang_to, 20, "")

    logging.info("\nWeighted {0} pages to:".format(args.language_stats))
    weight_by_pvs(lang_to, wd_pvs)
    print_stats(lang_to, 20, "", context_dict=wd_pvs)

    logging.info("\nLanguage pairs:")
    print_stats(to_from, 30, "")

    logging.info("\nWeighted language pairs:")
    weighted_to_from = weight_by_proj(to_from, proj_pvs)
    print_stats(weighted_to_from, 20, "", context_dict=to_from)

    logging.info("\nTop-viewed WD items:")
    print_stats(wd_pvs, 100, "")

    if wd_examples:
        for wditem in wd_examples:
            logging.info('{0} ({1}):'.format(wditem, wd_to_entitle[wditem]))
            print_stats(wd_examples[wditem], threshold=20, lbl="", context_dict=to_from)


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