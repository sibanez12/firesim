#!/usr/bin/env python2

import os
import argparse
import pandas as pd

def parse_switchlog(switchlog):
    with open(switchlog, 'r') as f:
            readlines = f.readlines()
    qsize_stats = {"time":[], "port":[], "hp_bytes":[], "lp_bytes":[]}
    event_stats = {"event":[], "time":[], "port":[]}
    for line in readlines:
        if "&&CSV&&QueueSize" in line:
            data = line.split(',')
            qsize_stats["time"].append(float(data[1]))
            qsize_stats["port"].append(float(data[2]))
            qsize_stats["hp_bytes"].append(float(data[3]))
            qsize_stats["lp_bytes"].append(float(data[4]))
        elif "&&CSV&&Events" in line:
            data = line.split(',')
            event_stats["event"].append(data[1])
            event_stats["time"].append(float(data[2]))
            event_stats["port"].append(float(data[3]))
    return qsize_stats, event_stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('switchlog', type=str, help='The switchlog to parse')
    args = parser.parse_args()

    qsize_stats, event_stats = parse_switchlog(args.switchlog)

    # write stats to CSV files
    outdir = os.path.dirname(args.switchlog)
    qsize_df = pd.DataFrame(qsize_stats, dtype=float)
    event_df = pd.DataFrame(event_stats, dtype=float)
    with open(os.path.join(outdir, 'qsize.csv'), 'w') as f:
        f.write(qsize_df.to_csv(index=False))
    with open(os.path.join(outdir, 'events.csv'), 'w') as f:
        f.write(event_df.to_csv(index=False))

if __name__ == '__main__':
    main()

