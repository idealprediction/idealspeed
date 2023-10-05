""" performance tests for read/write io for time series
    run "py.test" in the command line to run tests
    run "py.test -s" to disable stdout capture to view the file size results

    notes:
    - as of pandas 2.1, parquet compression options are 'snappy’, ‘gzip’, ‘brotli’, ‘lz4’, ‘zstd’
"""

from functools import partial
import os
import pathlib
from typing import Optional

import pandas as pd


# global variables for the data and cache paths
dirname, filename = os.path.split(os.path.abspath(__file__))
path = pathlib.Path(dirname)
CACHE_PATH = path.parent / "cache"
DATA_PATH = path.parent / "data"  # path.joinpath("../data")

# test pd.DataFrame
DF_TEST = pd.read_pickle(DATA_PATH / "ts_float.pkl")

# file format and compression to test
TEST_FORMATS = ["csv", "parquet", "pickle"]
TEST_COMPRESSIONS = [None, "gzip", "xz", "zstd"]


def get_file_name(format: str, compression: Optional[str]) -> pathlib.Path:
    """file name for the pd.DataFrame with the specified format and compression"""
    file_name = f"ts.{format}_{str(compression)}.{format}"
    if compression:
        file_name += f".{compression}"
    return CACHE_PATH / file_name


def read_df(format: str, compression: Optional[str]) -> pd.DataFrame:
    """write a file with a specified format and compression"""
    file_name = get_file_name(format=format, compression=compression)
    df = pd.DataFrame()
    match format:
        case "csv":
            df = pd.read_csv(file_name, compression=compression)
        case "parquet":
            df = pd.read_parquet(file_name, engine="pyarrow")
        case "pickle":
            df = pd.read_pickle(file_name, compression=compression)
        case _:
            raise Exception(f"unknown format {format}")
    return df


def write_df(df: pd.DataFrame, format: str, compression: Optional[str]):
    """write a file with a specified format and compression"""
    file_name = get_file_name(format=format, compression=compression)

    # write the df
    match format:
        case "csv":
            df.to_csv(file_name, compression=compression)
        case "parquet":
            df.to_parquet(file_name, compression=compression)
        case "pickle":
            df.to_pickle(file_name, compression=compression)
        case _:
            raise Exception(f"unknown format {format}")


def test_setup_timeseries():
    """setup: read the original source DataFrame and
    save to various file formats in the cache
    """
    # create a path for DataFrames, if needed
    print(f"setup is using data from a pd.DataFrame with {len(DF_TEST)} rows")
    print(f"setup is writing pd.DataFrame files to path = {str(CACHE_PATH)}")
    CACHE_PATH.mkdir(exist_ok=True)

    # write file versions, skipping parquet/xz
    for fmt in TEST_FORMATS:
        for compression in TEST_COMPRESSIONS:
            if fmt == "parquet" and compression == "xz":
                continue
            write_df(df=DF_TEST, format=fmt, compression=compression)


# these functions use the benchmark fixture in py.test
# see https://github.com/ionelmc/pytest-benchmark
# -- CSV ---
def test_read_csv_none(benchmark):
    func = partial(read_df, "csv", None)
    benchmark(func)


def test_read_csv_gzip(benchmark):
    func = partial(read_df, "csv", "gzip")
    benchmark(func)


def test_read_csv_xz(benchmark):
    func = partial(read_df, "csv", "xz")
    benchmark(func)


def test_read_csv_zstd(benchmark):
    func = partial(read_df, "csv", "zstd")
    benchmark(func)


def test_write_csv_none(benchmark):
    func = partial(write_df, DF_TEST, "csv", None)
    benchmark(func)


def test_write_csv_gzip(benchmark):
    func = partial(write_df, DF_TEST, "csv", "gzip")
    benchmark(func)


def test_write_csv_xz(benchmark):
    func = partial(write_df, DF_TEST, "csv", "xz")
    benchmark(func)


def test_write_csv_zstd(benchmark):
    func = partial(write_df, DF_TEST, "csv", "zstd")
    benchmark(func)


# --- PARQUET ---
def test_read_parquet_none(benchmark):
    func = partial(read_df, "parquet", None)
    benchmark(func)


def test_read_parquet_gzip(benchmark):
    func = partial(read_df, "parquet", "gzip")
    benchmark(func)


def test_read_parquet_zstd(benchmark):
    func = partial(read_df, "parquet", "zstd")
    benchmark(func)


def test_write_parquet_none(benchmark):
    func = partial(write_df, DF_TEST, "parquet", None)
    benchmark(func)


def test_write_parquet_gzip(benchmark):
    func = partial(write_df, DF_TEST, "parquet", "gzip")
    benchmark(func)


def test_write_parquet_zstd(benchmark):
    func = partial(write_df, DF_TEST, "parquet", "zstd")
    benchmark(func)


# --- PICKLE ---
def test_read_pickle_none(benchmark):
    func = partial(read_df, "pickle", None)
    benchmark(func)


def test_read_pickle_gzip(benchmark):
    func = partial(read_df, "pickle", "gzip")
    benchmark(func)


def test_read_pickle_xz(benchmark):
    func = partial(read_df, "pickle", "xz")
    benchmark(func)


def test_read_pickle_zstd(benchmark):
    func = partial(read_df, "pickle", "zstd")
    benchmark(func)


def test_write_pickle_none(benchmark):
    func = partial(write_df, DF_TEST, "pickle", None)
    benchmark(func)


def test_write_pickle_gzip(benchmark):
    func = partial(write_df, DF_TEST, "pickle", "gzip")
    benchmark(func)


def test_write_pickle_xz(benchmark):
    func = partial(write_df, DF_TEST, "pickle", "xz")
    benchmark(func)


def test_write_pickle_zstd(benchmark):
    func = partial(write_df, DF_TEST, "pickle", "zstd")
    benchmark(func)


def test_display_file_size():
    """show the file size in MB, rounded to 2 decimals, for each file type"""
    file_list = list(CACHE_PATH.glob("ts.*"))
    size_list = [(CACHE_PATH / fn).stat().st_size / 1e6 for fn in file_list]
    size_list = [round(sz, 2) for sz in size_list]
    file_dict = dict(zip(file_list, size_list))

    # show each file and size per row
    print("File Size (MB) = f(format, compression)\n")
    for file_name, file_size in file_dict.items():
        print(f"""{file_size} MB for {file_name.name}""")
