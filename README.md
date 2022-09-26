# 4_Cloud_Data_Lake

This repository contains all the files for the Cloud Data Lake project of the Data Engineer Nanodegree Program by Udacity.

## Introduction
"A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app."

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the dependecies (it is recommended to create a virtual environment before doing that).

```bash
pip install -r requirements.txt
```

## Configuration

We need to update the dl.cfg file with the AWS credentials:
```
AWS_ACCESS_KEY=<access_key>
AWS_SECRET_KEY=<secret_key>

S3_OUTPUT_BUCKET=<s3a://<bucket_name>/>
```

The S3_OUTPUT_BUCKET is where we are going to store the output of the script, contaning the parquet files of the tables

## Usage

```
python etl.py [--local]
```

This script will store the parquet files under the bucket . If we specify the --local option, everything will run using 
local directories (data and output by default).


### Running in local mode
```
unzip data/log-data.zip -d data
unzip data/song-data -d data
mkdir output
```

These 2 commands will unzip the sample data in order to be able to run the script.

After that, we are ready to run the etl.py:

```
python etl.py --local
```