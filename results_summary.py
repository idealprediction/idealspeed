# display the results from performance tests e.g. "py.test --benchmark-save"
#
# usage:
#   first run the tests and save benchmarks
#   >> py.test --benchmark-save=linux.20181001
#   then summarize the results
#   >> python results_summary.py .benchmarks/Darwin-CPython-2.7-64bit/0001_mac.20181001.json

import argparse
import json
import os
import pathlib

import pandas as pd

# global variable for the cache directory
CACHE_PATH = pathlib.Path("./cache")


def to_df(file_name):
    """convert stats to pandas DataFrame
    Args: file_name (str) JSON file to load
    Returns: df (DataFrame) converted stats
    """
    # load the JSON stats created by benchmark-save
    d = json.loads(open(file_name).read())

    # data --> pd.DataFrame
    name_list = [x["name"] for x in d["benchmarks"]]
    time_list = [int(x["stats"]["mean"] * 1e6) for x in d["benchmarks"]]
    df_readwrite = pd.DataFrame(index=name_list, data={"mean (us)": time_list})

    # pd.DataFrame with columns for read and write times
    df = df_readwrite[df_readwrite.index.str.contains("read_")]
    df.index = df.index.str.replace("test_read_", "")
    df_write = df_readwrite[df_readwrite.index.str.contains("write_")]
    df_write.index = df_write.index.str.replace("test_write_", "")
    df["write (us)"] = df_write

    # dictionary of file name: size
    file_list = list(CACHE_PATH.glob("ts*"))
    size_list = [round(fn.stat().st_size / 1e6, 2) for fn in file_list]
    file_list = [str(fn.name).split(".")[1] for fn in file_list]
    file_dict = dict(zip(file_list, size_list))

    # df for file size
    df_size = pd.DataFrame.from_dict(file_dict, orient="index")
    df_size.index = df_size.index.str.lower()

    # add column for file size to initial DataFrame
    df["size (MB)"] = df_size
    return df


def display_stats(df):
    """display stats stored in a pandas DataFrame
    Args: df (DataFrame) stats converted from JSON using to_df()
    """
    print(df.sort_values(by="mean (us)"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display benchmark stats")
    parser.add_argument("file", type=str, help="file name for the benchmark results")
    args = parser.parse_args()
    print("file = %s" % args.file)
    df = to_df(file_name=args.file)
    display_stats(df)
