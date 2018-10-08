# display the results from performance tests e.g. "py.test --benchmark-save"
#
# usage: 
#   first run the tests and save benchmarks
#   >> py.test --benchmark-save=mac.20181001 
#   then summarize the results
#   >> python results_summary.py .benchmarks/Darwin-CPython-2.7-64bit/0001_mac.20181001.json 

import argparse
import json
import os
import pandas as pd

# global variable for the cache directory
CACHE_PATH = './cache'

def to_df(file_name):
    """ convert stats to pandas DataFrame 
        Args: file_name (str) JSON file to load
        Returns: df (DataFrame) converted stats
    """
    # load the JSON stats created by benchmark-save
    d = json.loads(open(file_name).read())

    # data --> pd.DataFrame
    name_list = [x['name'] for x in d['benchmarks']]
    time_list = [int(x['stats']['mean'] * 1e6) for x in d['benchmarks']]
    df = pd.DataFrame(index=name_list, data={'mean (us)': time_list})
    df.index = df.index.str.replace('test_read_', '')

    # dictionary of file name: size
    file_list = os.listdir(CACHE_PATH)
    file_list = [fn for fn in file_list if 'ts_' in fn]
    size_list = [os.path.getsize(os.path.join(CACHE_PATH, fn)) / 1e6 for fn in file_list]
    size_list = [round(sz, 2) for sz in size_list]
    file_list = [fn for fn in file_list if 'ts_' in fn]
    file_dict = dict(zip(file_list, size_list))

    # df for file size
    df_size = pd.DataFrame.from_dict(file_dict, orient='index')
    df_size.index = df_size.index.str.split('.').str[0]

    # add column for file size to initial DataFrame
    df['size (MB)'] = df_size
    return df


def display_stats(df):
    """ display stats stored in a pandas DataFrame 
        Args: df (DataFrame) stats converted from JSON using to_df()
    """
    print(df.sort_values(by='mean (us)'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Display benchmark stats')
    parser.add_argument('file', type=str, help='file name for the benchmark results')
    args = parser.parse_args()
    print('file = %s' % args.file)
    df = to_df(file_name=args.file)
    display_stats(df)

