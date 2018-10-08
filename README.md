# Ideal Speed [PUBLIC] 
Tests the performance of Python data libraries. 

## OVERVIEW
Ideal Prediction is a company focused on data science, mainly in the financial industry. This repository contains code we used to test the performance of libraries that are core to our software.

## INSTALL
1. Snappy library for fast compression
```
sudo apt-get install libsnappy-dev # APT
sudo yum install libsnappy-devel # RPM
brew install snappy # mac / brew
```
For windows, reference this post: [stackoverflow install snappy windows](https://stackoverflow.com/questions/42979544/how-to-install-snappy-c-libraries-on-windows-10-for-use-with-python-snappy-in-an?rq=1)
1. Python requirements
```pip install -r requirements.txt```

## PERFORMANCE TESTS
The following performance tests are available:
1. File types - Test the performance of various file types, e.g. CSV, feather, and pickle, containing different column types (float, integer, timestamp, etc). 

### Run performance tests 
```
py.test
py.test -s # disables stdout capture so print results can be displayed
```

### Summarize test results
```
py.test --benchmark-save=test # first run the tests and save benchmarks
python results_summary.py .benchmarks/Darwin-CPython-2.7-64bit/0001_test.json # summarize results
```

## LICENSE
This code is made publically available under an MIT license to help the general data science community.
