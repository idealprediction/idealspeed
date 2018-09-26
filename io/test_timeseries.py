# performance tests for read/write io for time series
# run "py.test" in the command line to run tests
# run "py.test -s" to disable stdout capture to see the file size results
import os
import pandas as pd
import pystore
from time import sleep

WITH_NETWORK_SIMULATION = True
S3_SPEED = 20000000.0

def test_setup_timeseries():
    """ setup: read the original source DataFrame and 
        save to various file formats in the cache 
    """
    # read source DataFrame - index: datetime[64], columns: bid (float), ask (float)
    df = pd.read_pickle('../data/ts_float32.pkl')

    # write to cache
    path = '../cache'
    if not os.path.exists(path):
        os.makedirs(path)

    df.to_csv('../cache/ts_float.csv')
    df.to_csv('../cache/ts_float.csv.gz', compression='gzip')
    df.to_parquet('../cache/ts_float.parq')
    df.to_parquet('../cache/ts_float.parq.gz', compression='gzip')
    df.to_parquet('../cache/ts_float.parq.snappy', compression='snappy')
    df.to_pickle('../cache/ts_float.pkl')
    df.to_pickle('../cache/ts_float.pkl.gz', compression='gzip')
    df.to_hdf('../cache/ts_float_no_compr.hdf', key='df', complevel=0)
    df.to_hdf('../cache/ts_float_snappy.hdf', key='df', complib='blosc:snappy', complevel=9)
    df.to_hdf('../cache/ts_float_lz4.hdf', key='df', complib='blosc:lz4', complevel=9)

def read_ts_float_csv():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_csv('../cache/ts_float.csv')

def read_ts_float_csv_gz():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_csv('../cache/ts_float.csv.gz', compression='gzip')

def read_ts_float_pickle():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_pickle('../cache/ts_float.pkl')

def read_ts_float_pickle_gz():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_pickle('../cache/ts_float.pkl.gz', compression='gzip')

def read_ts_float_parquet():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_parquet('../cache/ts_float.parq')

def read_ts_float_parquet_gz(nthreads=1):
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_parquet('../cache/ts_float.parq.gz', engine='pyarrow', nthreads=nthreads)

def read_ts_float_parquet_snappy(nthreads=1):
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_parquet('../cache/ts_float.parq.snappy', engine='pyarrow', nthreads=nthreads)

def read_ts_float_hdf_no_compr():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_hdf('../cache/ts_float_no_compr.hdf')

def read_ts_float_hdf_snappy():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_hdf('../cache/ts_float_snappy.hdf')

def read_ts_float_hdf_lz4():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float.pkl')
        sleep(file_size/S3_SPEED)
    df = pd.read_hdf('../cache/ts_float_lz4.hdf')

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

def test_read_ts_float_parquet_snappy(benchmark):
    benchmark(read_ts_float_parquet_snappy)

def test_read_ts_float_parquet_gz_4_threads(benchmark):
    benchmark(read_ts_float_parquet_gz, 4)

def test_read_ts_float_hdf_no_compr(benchmark):
    benchmark(read_ts_float_hdf_no_compr)

def test_read_ts_float_hdf_snappy(benchmark):
    benchmark(read_ts_float_hdf_snappy)

def test_read_ts_float_hdf_lz4(benchmark):
    benchmark(read_ts_float_hdf_lz4)

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
