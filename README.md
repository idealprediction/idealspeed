# Ideal Speed [PUBLIC] 
Tests the performance of Python data libraries. 

## OVERVIEW
Ideal Prediction is a company focused on data science, mainly in the financial industry. This repository contains code we used to test the performance of libraries that are core to our software.

## INSTALL
Python requirements
```pip install -r requirements.txt```

## PERFORMANCE TESTS
These steps run the performance tests and summarize the results

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
