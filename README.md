# iobeam client tools

**[iobeam](https://iobeam.com)** is a data platform for connected devices.

For more information on iobeam, please [check out our documentation](https://docs.iobeam.com).

Please note that we are currently invite-only. You will need an invite
to generate a valid token and use our APIs.
([Sign up here](https://iobeam.com) for an invite.)*

## Before you start

Before you can start sending data to the iobeam backend, you'll need a
`project_id` and  `project_token` (with write-access enabled) for a valid
**iobeam** account. You can get these easily with our
[command-line interface (CLI) tool](https://github.com/iobeam/iobeam) or by
accessing your project settings from [our web app](https://app.iobeam.com).

You currently need python **2.7.9+** for this script.

## Installation

First, install the iobeam python library:

    pip install iobeam

For more information or any problems, see [its github reposistory](https://github.com/iobeam/iobeam-client-python).

Then, to get the tools directory:

    git clone https://github.com/iobeam/iobeam-client-tools.git

## Overview: Uploading data to iobeam Cloud

This repository includes a tool for uploading data to the iobeam
platform.  It allows you to upload data from multiple input files,
with each file representing a different device.  It doesn't provide
real parallelism, but round-robins data batches across the different
devices.  That is, if the batch size (`--rows`) is 10, it'll first
send 1-10 for device 1, 1-10 for device 2, etc., then rows 11-20 for
device 1, 11-20 for device 2, etc.

If one of the data files' columns is an integer `timestamp`, it will
use that value as the timestamp for each data row.  If that timestamp
is not in milliseconds, you need to specify its fidelity from the
command line (`--ts`).

If no timestamp column is provided -- which we expect to be the norm
-- it will use the current local system time as the basis for sending
a batch of data. Currently, it will align the timestamps across the
rows of different files (devices), as well as smooth out timestamps in
a batch. In particular, if the delay between batches (`--delay`) is
1000ms, and the batch size is 10 rows, each row is given a timestamp
that is 100ms apart (with the first element of the batch set to
current system time).

In order to send numeric or boolean data, you must explicitly specify
those data types in the file header metadata, as detailed below.  From
the type of data sent to the iobeam Cloud, it infers a loose data
schema on the data.  This is across the entire project, so you can
compare `temperature` data from one device to another.  For that
reason, the type (string, numeric, or boolean) of a specific series
name (given as a column in the CSV files) must be identical across the
project.  In other words, some devices can't send a `temperature` as a
string, while others send as a numeric.  For performance reasons, we
suggest using sending numeric data whenever appropriate.

## Running the data uploader

First, either edit the top of the data-uploader.py script to hard-code
your projectID and token, or include them as command-line arguments
whenever you run the script.

```text
$ python data-uploader.py -h
usage: data-uploader.py [-h] [-v] [--pid PROJECT_ID] [--did DEVICE_ID]
                        [--token TOKEN] [--ts TIMESTAMP] [--xmit XMIT_COUNT]
                        [--rows ROWS_PER] [--delay DELAY_BW]
                        [--null-string NULL_STRING] [--skip-invalid]
                        input_file [input_file ...]

Upload data to iobeam Cloud.

Required input file(s) should be in CSV form, with the header providing
metadata detailing the column names and types, and optionally device
ID/name information:

    # This is a comment
    ! device_id: DEV123
    ! device_name: bored-panda-152
    ! columns: col1, col2 ,col3, ..., colN
    
If one of the columns is 'timestamp', this integer value is used
as the row's timestamp when uploading data to iobeam. Otherwise,
the current time is used. If a timestamp is provided in the data,
its granularity (sec,msec,usec) should be specified as a program arg.

As CSV input does not have type information (compared to JSON, for example),
column types must be specified in header information, as either strings (s),
numbers (n), or booleans (b). Type information should be given in brackets
after the column name, e.g.,

  ! columns: timestamp[n], name[s], temperature[n]

If no type information is provided, a string is assumed:

  ! columns: name, temperature[n]

Column names must be alphanumeric or use special characters ('_', '-').
Names are interpreted in case insensitive fashion. Reserved names 'time',
'time_offset', and 'all' are not allowed.

Device IDs must be specified in metadata headers if multiple input files
are provided.  If a single input file is provided, device IDs can be
specified either in metadata, from the command line, or the script
will auto-assign.

positional arguments:
  input_file            input file(s)

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  --pid PROJECT_ID      iobeam project ID
  --did DEVICE_ID       iobeam device ID, auto-generated if not supplied
  --token TOKEN         iobeam token
  --ts TIMESTAMP        timestamp fidelity: sec, msec, usec (default: msec)
  --xmit XMIT_COUNT     number of times to transmit file (continuously: 0, default: 1)
  --rows ROWS_PER       rows sent per batch (default: 10)
  --delay DELAY_BW      delay in msec between sending data batches (default: 1000)
  --null-string NULL_STRING
                        case-insensitive string to represent null element (default: null)
  --skip-invalid        skip invalid rows from input (otherwise exits with error)

```

The required input file should have one of the two forms, where the
first uses the current time when writing each value as its timestamp,
and the latter uses a timestamp present in the input file.  Note the
supplied timestamp must be an interger value, and sec/msec/usec since
the UNIX epoch.

```text
! device_id: MY_DEVICE_ID
! device_name: MY_DEVICE_NAME
! columns: temperature, pressure 
69.9320243493,705.780624978
73.5739249027,688.000063244
69.4814668231,762.891816604
72.7061546329,781.064519089
68.1670798008,736.497552655
```
or
```text
! device_id: MY_DEVICE_ID
! device_name: MY_DEVICE_NAME
! columns: timestamp, temperature, pressure
1450491262605,68.136342273,690.982706454
1450491263448,69.7090621185,730.146114884
1450491264649,68.3269429518,766.256505417
1450491265216,66.410675139,709.655026995
1450491266946,71.8243109375,752.015438456
```