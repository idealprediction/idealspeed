# performance tests for read/write io for time series
# run "py.test" in the command line to run tests
# run "py.test -s" to disable stdout capture to see the file size results
import os
import pandas as pd
import numpy as np
import lz4.frame

# global variables for the data and cache paths
dirname, filename = os.path.split(os.path.abspath(__file__))
CACHE_PATH = os.path.join(dirname, '../cache')
DATA_PATH = os.path.join(dirname, '../data')

# global variables for simulating network time (not currently used)
# from time import sleep
# WITH_NETWORK_SIMULATION = False
# S3_SPEED = 20000000.0

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


    # numpy: Construct an array in the form [[column names, index(timestamp), values]]
    column_names = np.array(np.insert(df.columns.values, 0, df.index.name), dtype='string')
    index = np.array(df.index, dtype='datetime64')
    data = np.array(df.values, dtype='float')
    np.save(os.path.join(CACHE_PATH, 'ts_float_numpy.npy'), np.array([column_names, index, data]))

    # TODO: pd.DataFrame.to_hdf() hangs the process
    # df.to_hdf('../cache/ts_float_no_compr.hdf', key='df', complevel=0)
    # df.to_hdf('../cache/ts_float_snappy.hdf', key='df', complib='blosc:snappy', complevel=9)
    # df.to_hdf('../cache/ts_float_lz4.hdf', key='df', complib='blosc:lz4', complevel=9)

    # # hdf5 numpy array
    # h5f = h5py.File(os.path.join(CACHE_PATH, 'ts_float_numpy.h5'), 'w')
    # h5f.create_dataset('data', data=np.array([column_names, df.reset_index().values]))
    # h5f.close()

    # Convert datetime64 to int
    df.index = df.index.astype(int)
    df.to_pickle(os.path.join(CACHE_PATH, 'ts_float_int_timestamp.pkl'))

    column_names = np.array(np.insert(df.columns.values, 0, df.index.name), dtype='string')
    index = np.array(df.index, dtype='int')
    data = np.array(df.values, dtype='float')
    np.save(os.path.join(CACHE_PATH, 'ts_float_numpy_int_datetime.npy'), np.array([column_names, index, data]))


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

def read_ts_float_numpy():
    fn = os.path.join(CACHE_PATH, 'ts_float_numpy.npy')
    numpy_matrix = np.load(fn)

    # Deconstruct the dataframe from numpy array with column names and index
    df = pd.DataFrame(data=numpy_matrix[2], columns=numpy_matrix[0][1:], index=numpy_matrix[1])
    df.index.names = [numpy_matrix[0][0]]

def read_ts_float_hdf_no_compr():
    fn = os.path.join(CACHE_PATH, 'ts_float_no_compr.hdf')
    df = pd.read_hdf(fn)


def read_ts_float_hdf_snappy():
    fn = os.path.join(CACHE_PATH, 'ts_float_snappy.hdf')
    df = pd.read_hdf(fn)


def read_ts_float_hdf_lz4():
    fn = os.path.join(CACHE_PATH, 'ts_float_lz4.hdf')
    df = pd.read_hdf(fn)

def read_ts_float_numpy_hdf():
    fn = os.path.join(CACHE_PATH, 'ts_float_numpy.h5')
    h5f = h5py.File(fn, 'r')
    numpy_matrix = h5f['data'][:]
    df = pd.DataFrame(data=numpy_matrix[1][:, 1:], columns=numpy_matrix[0][1:], index=numpy_matrix[1][:, 0])
    df.index.names = [numpy_matrix[0][0]]

def read_ts_pickle_int_datetime():

    df = pd.read_pickle(os.path.join(CACHE_PATH, 'ts_float_int_timestamp.pkl'))
    df.index = pd.to_datetime(df.index)

def read_ts_pickle_int_datetime_no_conversion():

    df = pd.read_pickle(os.path.join(CACHE_PATH, 'ts_float_int_timestamp.pkl'))

def read_ts_float_numpy_int_datetime():
    fn = os.path.join(CACHE_PATH, 'ts_float_numpy_int_datetime.npy')
    numpy_matrix = np.load(fn)

    # Deconstruct the dataframe from numpy array with column names and index
    df = pd.DataFrame(data=numpy_matrix[2], columns=numpy_matrix[0][1:], index=numpy_matrix[1])
    df.index.names = [numpy_matrix[0][0]]

# these functions use the benchmark fixture in py.test
# see https://github.com/ionelmc/pytest-benchmark
def test_read_ts_float_csv(benchmark):
    benchmark(read_ts_float_csv)


def test_read_ts_float_csv_gz(benchmark):
    benchmark(read_ts_float_csv_gz)

def test_read_ts_float_numpy(benchmark):
    benchmark(read_ts_float_numpy)

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

def test_read_ts_int_datetime(benchmark):
    benchmark(read_ts_pickle_int_datetime)

def test_read_ts_int_datetime_no_conversion(benchmark):
    benchmark(read_ts_pickle_int_datetime_no_conversion)

def test_read_ts_float_numpy_int_datetime(benchmark):
    benchmark(read_ts_float_numpy_int_datetime)

# def test_read_ts_float_numpy_hdf(benchmark):
#     benchmark(read_ts_float_numpy_hdf)

# def test_read_ts_float_hdf_no_compr(benchmark):
#     benchmark(read_ts_float_hdf_no_compr)


# def test_read_ts_float_hdf_snappy(benchmark):
#     benchmark(read_ts_float_hdf_snappy)


# def test_read_ts_float_hdf_lz4(benchmark):
#     benchmark(read_ts_float_hdf_lz4)

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
