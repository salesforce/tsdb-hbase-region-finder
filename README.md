# regionfinder
[June 2019] A Python 2/3 CLI tool that takes in a metric name or expression and returns the HBase RegionServer and Region where it is located. Has a dependency on the HMaster Info Web UI.

Also available as a minimal webserver. 

- [regionfinder](#regionfinder)
  - [Setup (Python 2 - Mac - Recommended)](#setup-python-2---mac---recommended)
  - [CLI usage](#cli-usage)
  - [CLI Examples](#cli-examples)
  - [Webserver usage](#webserver-usage)
  - [Logging](#logging)
  - [Development](#development)
## Setup (Python 2 - Mac - Recommended)
Prerequisites: `python` version 2, `pip`, and `virtualenv`
```
# If you don't already have pip
brew install python@2

pip install virtualenv

python --version
## This should output Python 2.X.X , if not, you may need to move /usr/local/bin 
## to the front of your path and restart terminal
```

Clone the repository, create a Python virtualenv, and install local dependencies
```
git clone https://github.com/salesforce/tsdb-hbase-region-finder.git
cd tsdb-hbase-region-finder

virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
# Install the local regionfinder module
pip install -e .
```

## CLI usage
```
$ bin/cli -h
usage: cli.py [-h] [-t TIME] [-c CONFIG] expression

Outputs CSV-formatted region servers and names of all matching timeseries
(delimited by the | character )

positional arguments:
  expression            The TSDB metric name

optional arguments:
  -h, --help            show this help message and exit
  -t TIME, --time TIME  TSDB relative time-string or absolute epoch time to
                        use in rowkey. See http://opentsdb.net/docs/build/html
                        /user_guide/query/dates.html
  -c CONFIG, --config CONFIG
                        Path to config yaml
```

## CLI Examples
```
$ bin/cli envoy.server.uptime
RegionServer|RegionName
http://host1.hbase.com:60030/|tsdb,1510122330068.05714303d5f455bfac661199d2cbb343
http://host1.hbase.com:60030/|tsdb,1510122330068.05714303d5f455bfac661199d2cbb343
```

## Webserver usage
```
$ bin/server -h
usage: server.py [-h] [-c CONFIG] [-p PORT]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        path to config yaml
  -p PORT, --port PORT  path to config yaml
```

## Logging
The logger, by default, appends INFO level logs to `rf.log`. The desired log file location can be set with the env variable `REGION_FINDER_LOG`.

The CLI's stream logger level is `ERROR`

The web server's stream logger level is `INFO`

## Development
Install additional dev dependencies
```
pip install -r requirements-dev.txt
```
Run tests for Python versions - this can be done in a Python 2 or 3 virtualenv
```
tox 
```
