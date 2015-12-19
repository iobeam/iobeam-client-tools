# iobeam client tools

**[iobeam](https://iobeam.com)** is a data platform for connected devices.

For more information on iobeam, please [check out our documentation](https://docs.iobeam.com).

*Please note that we are currently invite-only. You will need an invite
to generate a valid token and use our APIs.
([Sign up here](https://iobeam.com) for an invite.)*

## Before you start

Before you can start sending data to the iobeam backend, you'll need a
`project_id` and  `project_token` (with write-access enabled) for a valid
**iobeam** account. You can get these easily with our
[command-line interface (CLI) tool](https://github.com/iobeam/iobeam) or by
accessing your project settings from [our web app](https://app.iobeam.com).

You need python **2.7.9+** or **3.4.3+** (earlier versions of python3 may
work, but it has not been tested).

## Installation

First, install the iobeam python library:

    pip install iobeam

For more information or any problems, see [its github reposistory](https://github.com/iobeam/iobeam-client-python).

Then, to get the tools directory:

    git clone https://github.com/iobeam/iobeam-client-tool.git

## Uploading data to iobeam Cloud

This repository includes a tool for uploading data to the iobeam platform.

First, either edit the top of the data-uploader.py script to hard-code
your projectID and token, or include them as command-line arguments
whenever you run the script.

```text
$ python data-uploader.py -h

usage: data-uploader.py [-h] [-v] -i INPUT_FILE [--pid PROJECT_ID]
                        [--did DEVICE_ID] [--token TOKEN] [--ts TIMESTAMP]
                        [--repeat REPETITIONS] [--rows ROWS_PER]
                        [--delay DELAY_BW]

Upload data to iobeam Cloud. 

Required input file should be in CSV form, with the first line
providing the names of the columns in the form:

  #col1,col2,col3,...,colN

If one of the columns is 'timestamp', this integer value is used as
the row's timestamp when uploading data to iobeam. Otherwise, the
current time is used. If a timestamp is provided in the data, its
granularity (sec,msec,usec) should be specified as a program arg.

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -i INPUT_FILE         input file (required) (default: None)
  --pid PROJECT_ID      iobeam project ID (default: None)
  --did DEVICE_ID       iobeam device ID (auto-generated if not supplied)
                        (default: None)
  --token TOKEN         iobeam token (default: None)
  --ts TIMESTAMP        timestamp fidelity (sec, msec, usec) (default: msec)
  --repeat REPETITIONS  number of times to transmit file (0 = continuously)
                        (default: 1)
  --rows ROWS_PER       rows sent per iteration (default: 10)
  --delay DELAY_BW      delay between sending iteration (in milliseconds)
                        (default: 1000)
```

The required input file should have one of the two forms, where the
first uses the current time when writing each value as its timestamp,
and the latter uses a timestamp present in the input file.  Note the
supplied timestamp must be an interger value, and sec/msec/usec since
the UNIX epoch.

```text
# temperature, pressure 
69.9320243493,705.780624978
73.5739249027,688.000063244
69.4814668231,762.891816604
72.7061546329,781.064519089
68.1670798008,736.497552655
```
or
```text
# timestamp, temperature, pressure
1450491262605,68.136342273,690.982706454
1450491263448,69.7090621185,730.146114884
1450491264649,68.3269429518,766.256505417
1450491265216,66.410675139,709.655026995
1450491266946,71.8243109375,752.015438456
```

