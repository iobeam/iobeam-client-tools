import argparse
import time
import sys

from iobeam import iobeam

###############################################################
# Set project ID and token in source code, or from command line
###############################################################

IOBEAM_PROJECT_ID = None 
IOBEAM_TOKEN = None

###############################################################

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

###############################################################

class Config:
    def __init__(self, args):
        # Do not modify
        self.args = args
        self.iobeamClient = None
        self.iobeamDataStore = None

        self.format = None
        self.formatWithoutTimestamp = None
        self.timestampIndex = -1
        self.timestampFormat = None
        self.multiplier = None


def addData(config, data):

    if len(config.format) != len(data):
        print "\nError: Number of data elements does not match format"
        print "\tFormat:\t%s" % config.format
        print "\tData:\t%s" % data
        sys.exit(1)

    if config.timestampIndex < 0:
        now = int(time.time() * config.multiplier)
        ts = iobeam.Timestamp(now, config.timestampFormat)
        config.iobeamDataStore.add(ts, dict(zip(config.format, data)))
    else:
        now = int(data[config.timestampIndex])
        ts = config.timestampFormat(now, config.timestampFormat)
        del data[config.timestampIndex]
        config.iobeamDataStore.add(ts, dict(zip(config.formatWithoutTimestamp, data)))


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


def analyzeFile(config):
    try:
        with open(config.args.input_file, 'r') as f:
            # first_line is formatting.  Read past it.
            f.readline()

            cnt = config.args.rows_per
            for line in f:
                addData(config, getDataLine(line))
                if config.args.rows_per > 0 and cnt == 0:
                    config.iobeamClient.send()
                    print "Sent data batch to iobeam"
                    time.sleep((config.args.delay_bw / 1000.0))
                    cnt = config.args.rows_per
                cnt -= 1

            config.iobeamClient.send()
            print "Sent data batch to iobeam"
            time.sleep((config.args.delay_bw / 1000.0))


    except (OSError, IOError) as e:
        _parser.print_usage()
        print "\nError reading file."
        sys.exit(1)


def extractFileFormat(config):
    try:
        with open(config.args.input_file, 'r') as f:
            first_line = f.readline()
            config.format = getDataFormat(first_line)

            if 'timestamp' in config.format:
                config.timestampIndex = config.format.index('timestamp')
                config.formatWithoutTimestamp = list(config.format)
                del config.formatWithoutTimestamp[config.timestampIndex]

    except (OSError, IOError) as e:
        _parser.print_usage()
        print "\nError reading file"
        sys.exit(1)


def checkArgs(config):

    error = None

    if config.args.project_id == None:
        error = "Unknown project ID"
    elif config.args.token == None:
        error = "Unknown project token"
    elif config.args.rows_per < 0:
        error = "Number of rows must be >= 0"
    elif config.args.delay_bw < 0:
        error = "Delay must be >= 0 milliseconds"
    elif config.args.repetitions < 0:
        error = "Repetitions must be >= 0"
    else:
        config.args.timestamp = config.args.timestamp.lower();
        if config.args.timestamp == 'sec':
            config.timestampFormat = iobeam.TimeUnit.SECONDS
            config.multiplier = 1
        elif config.args.timestamp == 'msec':
            config.timestampFormat = iobeam.TimeUnit.MILLISECONDS
            config.multiplier = 1000
        elif config.args.timestamp == 'usec':
            config.timestampFormat = iobeam.TimeUnit.MICROSECONDS
            config.multiplier = 1000000
        else:
            error = "Timestamp must be 'sec', 'msec', or 'usec'"

    if error != None:
        _parser.print_usage()
        print "\nError: %s" % error
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


    config = Config(_parser.parse_args())
    checkArgs(config)
    extractFileFormat(config)

    builder = iobeam.ClientBuilder(config.args.project_id, config.args.token) \
        .setBackend("https://api-dev.iobeam.com/v1/") \
        .saveToDisk()

    if config.args.device_id != None:
        builder.registerOrSetId(deviceId=config.args.device_id)
    else:
        builder.registerDevice()

    config.iobeamClient = builder.build()
    config.iobeamDataStore = config.iobeamClient.createDataStore(config.format)

    if config.args.repetitions == 0:
        analyzeFile(config)
    elif config.args.repetitions > 0:
        while config.args.repetitions > 0:
            analyzeFile(config)
            config.args.repetitions -= 1
