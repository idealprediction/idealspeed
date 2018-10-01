# performance tests for read/write io for time series
# run "py.test" in the command line to run tests
# run "py.test -s" to disable stdout capture to see the file size results
import os
import pandas as pd
import numpy as np
import pystore
from time import sleep
import h5py
import lz4.frame

WITH_NETWORK_SIMULATION = True
S3_SPEED = 20000000.0

def snappy_compress(path):
        path_to_store = path+'.snappy'

        with open(path, 'rb') as in_file:
          with open(path_to_store, 'w') as out_file:
            snappy.stream_compress(in_file, out_file)
            out_file.close()
            in_file.close()

        return path_to_store


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
    np.save('../cache/ts_float_numpy.npy', df.values)

    # hdf5 numpy array
    h5f = h5py.File('../cache/ts_float_numpy.h5', 'w')
    h5f.create_dataset('data', data=df.values)
    h5f.close()

    #lz4 numpy array
    with lz4.frame.open('../cache/ts_float_numpy.lz4', mode='wb') as fp:
        bytes_written = fp.write(np.ascontiguousarray(df.values))

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

def read_ts_float_numpy():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float_numpy.npy')
        sleep(file_size/S3_SPEED)
    numpy_matrix = np.load('../cache/ts_float_numpy.npy')
    df = pd.DataFrame(numpy_matrix)

def read_ts_float_numpy_hdf():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float_numpy.h5')
        sleep(file_size/S3_SPEED)

    h5f = h5py.File('../cache/ts_float_numpy.h5','r')
    numpy_matrix = h5f['data'][:]
    df = pd.DataFrame(numpy_matrix)


def read_ts_float_numpy_lz4():
    if WITH_NETWORK_SIMULATION:
        file_size = os.path.getsize('../cache/ts_float_numpy.lz4')
        sleep(file_size/S3_SPEED)

    with lz4.frame.open('../cache/ts_float_numpy.lz4', mode='r') as fp:
        output_data = fp.read()
        numpy_matrix = np.asfortranarray(output_data)
        df = pd.DataFrame(numpy_matrix)

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

def test_read_ts_float_numpy(benchmark):
    benchmark(read_ts_float_numpy)

def test_read_ts_float_numpy_hdf(benchmark):
    benchmark(read_ts_float_numpy_hdf)

def test_read_ts_float_numpy_lz4(benchmark):
    benchmark(read_ts_float_numpy_lz4)

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
