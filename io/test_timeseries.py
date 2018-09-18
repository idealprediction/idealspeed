# performance tests for read/write io for time series
# run "py.test" in the command line to run tests
# run "py.test -s" to disable stdout capture to see the file size results
import os
import pandas as pd


def test_setup_timeseries():
    """ setup: read the original source DataFrame and 
        save to various file formats in the cache 
    """
    # read source DataFrame - index: datetime[64], columns: bid (float), ask (float)
    df = pd.read_pickle('../data/ts_float.pkl')

    # write to cache
    path = '../cache'
    if not os.path.exists(path):
        os.makedirs(path)

    df.to_csv('../cache/ts_float.csv')
    df.to_csv('../cache/ts_float.csv.gz', compression='gzip')
    df.to_parquet('../cache/ts_float.parq')
    df.to_parquet('../cache/ts_float.parq.gz', compression='gzip')
    df.to_pickle('../cache/ts_float.pkl')
    df.to_pickle('../cache/ts_float.pkl.gz', compression='gzip')


def read_ts_float_csv():
    df = pd.read_csv('../cache/ts_float.csv')


def read_ts_float_csv_gz():
    df = pd.read_csv('../cache/ts_float.csv.gz', compression='gzip')


def read_ts_float_pickle():
    df = pd.read_pickle('../cache/ts_float.pkl')


def read_ts_float_pickle_gz():
    df = pd.read_pickle('../cache/ts_float.pkl.gz', compression='gzip')


def read_ts_float_parquet():
    df = pd.read_parquet('../cache/ts_float.parq')


def read_ts_float_parquet_gz(nthreads=1):
    df = pd.read_parquet('../cache/ts_float.parq.gz', engine='pyarrow', nthreads=nthreads)


# these functions use the benchmark fixture in py.test
# see https://github.com/ionelmc/pytest-benchmark
def test_read_ts_float_csv(benchmark):
    benchmark(read_ts_float_csv)


def test_read_ts_float_csv_gz(benchmark):
    benchmark(read_ts_float_csv_gz)


def test_read_ts_float_pickle(benchmark):
    benchmark(read_ts_float_pickle)


def test_read_ts_float_pickle_gz(benchmark):
    benchmark(read_ts_float_pickle_gz)


def test_read_ts_float_parquet(benchmark):
    benchmark(read_ts_float_parquet)


def test_read_ts_float_parquet_gz(benchmark):
    benchmark(read_ts_float_parquet_gz)


def test_read_ts_float_parquet_gz_4_threads(benchmark):
    benchmark(read_ts_float_parquet_gz, 4)


def test_display_file_size():
    """  show the file size for each file type """
    file_list = os.listdir('../cache')
    print file_list
    file_list = [fn for fn in file_list if 'ts_' in fn]
    print file_list
    size_list = [os.path.getsize(os.path.join('../cache', fn)) / 1e6 for fn in file_list]
    file_dict = dict(zip(file_list, size_list))
    print('File Size (MB)\n %s' % file_dict)
    # print('File Size (MB)\n')
    # for key, value in file_dict.iteritems():
    #     print('%s: %0.1f MB' % (key, value))
