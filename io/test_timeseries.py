# performance tests for read/write io for time series
# run "py.test" in the command line to run tests
# run "py.test -s" to disable stdout capture to see the file size results
import os
import pandas as pd
import numpy as np
import h5py
import lz4.frame

# global variables for the data and cache paths
dirname, filename = os.path.split(os.path.abspath(__file__))
CACHE_PATH = os.path.join(dirname, '../cache')
DATA_PATH = os.path.join(dirname, '../data')

# global variables for simulating network time (not currently used)
# from time import sleep
# WITH_NETWORK_SIMULATION = False
# S3_SPEED = 20000000.0

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
    df = pd.read_pickle(os.path.join(DATA_PATH, 'ts_float.pkl'))

    # create the cache path, if needed
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)

    # CSV
    df.to_csv(os.path.join(CACHE_PATH, 'ts_float.csv'))
    df.to_csv(os.path.join(CACHE_PATH, 'ts_float.csv.gz'), compression='gzip')

    # parquet
    df.to_parquet(os.path.join(CACHE_PATH, 'ts_float.parq'))
    df.to_parquet(os.path.join(CACHE_PATH, 'ts_float.parq.gz'), compression='gzip')
    df.to_parquet(os.path.join(CACHE_PATH, 'ts_float.parq.snappy'), compression='snappy')

    # pickle
    df.to_pickle(os.path.join(CACHE_PATH, 'ts_float.pkl'))
    df.to_pickle(os.path.join(CACHE_PATH, 'ts_float.pkl.gz'), compression='gzip')

    # TODO: pd.DataFrame.to_hdf() hangs the process
    # df.to_hdf('../cache/ts_float_no_compr.hdf', key='df', complevel=0)
    # df.to_hdf('../cache/ts_float_snappy.hdf', key='df', complib='blosc:snappy', complevel=9)
    # df.to_hdf('../cache/ts_float_lz4.hdf', key='df', complib='blosc:lz4', complevel=9)

    # numpy
    np.save(os.path.join(CACHE_PATH, 'ts_float_numpy.npy'), df.values)

    # hdf5 numpy array
    h5f = h5py.File(os.path.join(CACHE_PATH, 'ts_float_numpy.h5'), 'w')
    h5f.create_dataset('data', data=df.values)
    h5f.close()

    #lz4 numpy array
    with lz4.frame.open(os.path.join(CACHE_PATH, 'ts_float_numpy.lz4'), mode='wb') as fp:
        bytes_written = fp.write(np.ascontiguousarray(df.values))


# def network_simulation(filename):
#     if WITH_NETWORK_SIMULATION:
#         file_size = os.path.getsize(filename)
#         sleep(file_size / S3_SPEED)

def read_ts_float_csv():
    fn = os.path.join(CACHE_PATH, 'ts_float.csv')
    df = pd.read_csv(fn)


def read_ts_float_csv_gz():
    fn = os.path.join(CACHE_PATH, 'ts_float.csv.gz')
    df = pd.read_csv(fn, compression='gzip')


def read_ts_float_pickle():
    fn = os.path.join(CACHE_PATH, 'ts_float.pkl')
    df = pd.read_pickle(fn)


def read_ts_float_pickle_gz():
    fn = os.path.join(CACHE_PATH, 'ts_float.pkl.gz')
    df = pd.read_pickle(fn, compression='gzip')


def read_ts_float_parquet():
    fn = os.path.join(CACHE_PATH, 'ts_float.parq')
    df = pd.read_parquet(fn)


def read_ts_float_parquet_gz(nthreads=1):
    fn = os.path.join(CACHE_PATH, 'ts_float.parq.gz')
    df = pd.read_parquet(fn, engine='pyarrow', nthreads=nthreads)


def read_ts_float_parquet_snappy(nthreads=1):
    fn = os.path.join(CACHE_PATH, 'ts_float.parq.snappy')
    df = pd.read_parquet(fn, engine='pyarrow', nthreads=nthreads)


# def read_ts_float_hdf_no_compr():
#     fn = os.path.join(CACHE_PATH, 'ts_float_no_compr.hdf')
#     df = pd.read_hdf(fn)


# def read_ts_float_hdf_snappy():
#     fn = os.path.join(CACHE_PATH, 'ts_float_snappy.hdf')
#     df = pd.read_hdf(fn)


# def read_ts_float_hdf_lz4():
#     fn = os.path.join(CACHE_PATH, 'ts_float_lz4.hdf')
#     df = pd.read_hdf(fn)


def read_ts_float_numpy():
    fn = os.path.join(CACHE_PATH, 'ts_float_numpy.npy')
    numpy_matrix = np.load(fn)
    df = pd.DataFrame(numpy_matrix)


def read_ts_float_numpy_hdf():
    fn = os.path.join(CACHE_PATH, 'ts_float_numpy.h5')
    h5f = h5py.File(fn, 'r')
    numpy_matrix = h5f['data'][:]
    df = pd.DataFrame(numpy_matrix)


def read_ts_float_numpy_lz4():
    fn = os.path.join(CACHE_PATH, 'ts_float_numpy.lz4')
    with lz4.frame.open(fn, mode='r') as fp:
        output_data = fp.read()
        numpy_matrix = np.asfortranarray(output_data)
        df = pd.DataFrame(numpy_matrix)


# these functions use the benchmark fixture in py.test
# see https://github.com/ionelmc/pytest-benchmark
def test_read_ts_float_csv(benchmark):
    benchmark(read_ts_float_csv)


def test_read_ts_float_csv_gz(benchmark):
    benchmark(read_ts_float_csv_gz)


# def test_read_ts_float_hdf_no_compr(benchmark):
#     benchmark(read_ts_float_hdf_no_compr)


# def test_read_ts_float_hdf_snappy(benchmark):
#     benchmark(read_ts_float_hdf_snappy)


# def test_read_ts_float_hdf_lz4(benchmark):
#     benchmark(read_ts_float_hdf_lz4)


def test_read_ts_float_numpy(benchmark):
    benchmark(read_ts_float_numpy)


def test_read_ts_float_numpy_hdf(benchmark):
    benchmark(read_ts_float_numpy_hdf)


def test_read_ts_float_numpy_lz4(benchmark):
    benchmark(read_ts_float_numpy_lz4)


def test_read_ts_float_parquet(benchmark):
    benchmark(read_ts_float_parquet)


def test_read_ts_float_parquet_gz(benchmark):
    benchmark(read_ts_float_parquet_gz)


def test_read_ts_float_parquet_gz_4_threads(benchmark):
    benchmark(read_ts_float_parquet_gz, 4)


def test_read_ts_float_parquet_snappy(benchmark):
    benchmark(read_ts_float_parquet_snappy)


def test_read_ts_float_pickle(benchmark):
    benchmark(read_ts_float_pickle)


def test_read_ts_float_pickle_gz(benchmark):
    benchmark(read_ts_float_pickle_gz)


def test_display_file_size():
    """  show the file size in MB, rounded to 2 decimals, for each file type """
    file_list = os.listdir(CACHE_PATH)
    file_list = [fn for fn in file_list if 'ts_' in fn]
    size_list = [os.path.getsize(os.path.join(CACHE_PATH, fn)) / 1e6 for fn in file_list]
    size_list = [round(sz, 2) for sz in size_list]
    file_dict = dict(zip(file_list, size_list))
    print('File Size (MB)\n %s' % file_dict)

    # uncomment to show the each file and size per row
    # print('File Size (MB)\n')
    # for key, value in file_dict.iteritems():
    #     print('%s: %0.1f MB' % (key, value))
