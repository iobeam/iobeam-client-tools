import argparse
import time
import sys

from iobeam import iobeam

# To Edit
IOBEAM_PROJECT_ID = None
IOBEAM_TOKEN = None

_parser = argparse.ArgumentParser(version='0.1',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='''
Upload data to iobeam Cloud.

Required input file should be in CSV form, with the first
line providing the names of the columns in the form:

    #col1,col2,col3,...,colN.
    
If one of the columns is \'timestamp\', this integer value is used
as the row\'s timestamp when uploading data to iobeam. Otherwise,
the current time is used. If a timestamp is provided in the data,
its granularity (sec,msec,usec) should be specified as a program arg.

''')

# Do not modify
_iobeamClient = None
_iobeamDataStore = None

_format = None
_formatWithoutTimestamp = None
_timestampIndex = -1
_timestampFormat = None
_multiplier = None

def addData(data):
    global _iobeamDataStore
    global _format
    global _formatWithoutTimestamp
    global _timestampIndex
    global _multiplier

    if len(_format) != len(data):
        print "\nError: Number of data elements does not match format"
        print "\tFormat:\t%s" % _format
        print "\tData:\t%s" % data
        sys.exit(1)

    if _timestampIndex < 0:
        now = int(time.time() * _multiplier)
        ts = iobeam.Timestamp(now, _timestampFormat)
        _iobeamDataStore.add(ts, dict(zip(_format, data)))
    else:
        now = int(data[_timestampIndex])
        ts = _timestampFormat(now, _timestampFormat)
        del data[_timestampIndex]
        _iobeamDataStore.add(ts, dict(zip(_formatWithoutTimestamp, data)))


def getDataFormat(line):
    line = line.strip()
    if (len(line) == 0) or (line[0] != '#'):
        _parser.print_usage()
        print "\nError: First line of input file must be of form:"
        print "\t#col1,col2,col3,...,colN"
        sys.exit(1)

    return map((lambda x: x.strip()), line[1:].split(','))


def getDataLine(line):
    line = line.strip()
    return map((lambda x: x.strip()), line.split(','))


def analyzeFile(args):
    global _iobeamClient
    global _iobeamDataStore
    global _format
    global _formatWithoutTimestamp
    global _timestampIndex

    try:
        with open(args.input_file, 'r') as f:
            first_line = f.readline()
            if _iobeamDataStore == None or _format == None:
                _format = getDataFormat(first_line)
                _iobeamDataStore = _iobeamClient.createDataStore(_format)

                if 'timestamp' in _format:
                    _timestampIndex = _format.index('timestamp')
                    _formatWithoutTimestamp = list(_format)
                    del _formatWithoutTimestamp[_timestampIndex]

            cnt = args.rows_per
            for line in f:
                addData(getDataLine(line))
                if args.rows_per > 0 and cnt == 0:
                    _iobeamClient.send()
                    print "Sent data batch to iobeam"
                    time.sleep((args.delay_bw / 1000))
                    cnt = args.rows_per
                cnt -= 1

            _iobeamClient.send()
            print "Sent data batch to iobeam"
            time.sleep((args.delay_bw / 1000))


    except (OSError, IOError) as e:
        __parser.print_usage()
        print "\nError reading file."
        sys.exit(1)


if __name__ == "__main__":

    _parser.add_argument('-i', action='store', dest='input_file', required=True,
                        help='Input file (Required)')
    _parser.add_argument('--pid', action='store', dest='project_id', type=int,
                        help='iobeam project ID', default=IOBEAM_PROJECT_ID)
    _parser.add_argument('--did', action='store', dest='device_id',
                        help='iobeam device ID (auto-generated if not supplied)', default=None)
    _parser.add_argument('--token', action='store', dest='token',
                        help='iobeam token', default=IOBEAM_TOKEN)
    _parser.add_argument('--ts', action='store', dest='timestamp',
                        help='Granularity of timestamp (sec, msec, usec)', default='msec')
    _parser.add_argument('--repeat', action='store', dest='repetitions', type=int,
                        help='Number of times to transmit file (0 = continuously)', default=1)
    _parser.add_argument('--rows', action='store', dest='rows_per', type=int,
                        help='Rows sent per iteration (0 = all)', default=10)
    _parser.add_argument('--delay', action='store', dest='delay_bw', type=int,
                        help='Delay between sending iteration (in milliseconds)', default=1000)


    args = _parser.parse_args()

    if args.project_id == None:
        _parser.print_usage()
        print "\nError: Unknown project ID."
        sys.exit(1)

    if args.token == None:
        _parser.print_usage()
        print "\nError: Unknown project token."
        sys.exit(1)

    if args.rows_per < 0:
        _parser.print_usage()
        print "\nError: Number of rows must be >= 0."
        sys.exit(1)

    if args.delay_bw < 0:
        _parser.print_usage()
        print "\nError: Delay must be >= 0 milliseconds."
        sys.exit(1)

    args.timestamp = args.timestamp.lower();
    if args.timestamp == 'sec':
        _timestampFormat = iobeam.TimeUnit.SECONDS
        _multiplier = 1
    elif args.timestamp == 'msec':
        _timestampFormat = iobeam.TimeUnit.MILLISECONDS
        _multiplier = 1000
    elif args.timestamp == 'usec':
        _timestampFormat = iobeam.TimeUnit.MICROSECONDS
        _multiplier = 1000000
    else:
        _parser.print_usage()
        print "\nError: Timestamp must be 'sec', 'msec', or 'usec'."
        sys.exit(1)


    builder = iobeam.ClientBuilder(args.project_id, args.token) \
        .setBackend("https://api-dev.iobeam.com/v1/") \
        .saveToDisk()

    if args.device_id != None:
        builder.registerOrSetId(deviceId=args.device_id)
    else:
        builder.registerDevice()

    _iobeamClient = builder.build()

    if args.repetitions == 0:
        analyzeFile(args)
    elif args.repetitions > 0:
        while args.repetitions > 0:
            analyzeFile(args)
            args.repetitions -= 1
    else:
        _parser.print_usage()
        print "\nError: Repetitions must be >= 0."
        sys.exit(1)
