import argparse
import time
import sys

from iobeam import iobeam

###############################################################
# Set project ID and token in source code, or from command line
###############################################################

IOBEAM_PROJECT_ID = None 
IOBEAM_TOKEN = None

COMMENT_CHAR = '#'
METADATA_CHAR = '!'

###############################################################

_parser = argparse.ArgumentParser(version='0.2',
                                 formatter_class=argparse.RawTextHelpFormatter,
                                 description='''
Upload data to iobeam Cloud.

Required input file(s) should be in CSV form, with the header providing
metadata detailing the columns, and optionally device ID/name information:

    # This is a comment
    ! device_id: DEV123
    ! device_name: bored-panda-152
    ! columns: col1, col2 ,col3, ..., colN
    
If one of the columns is \'timestamp\', this integer value is used
as the row\'s timestamp when uploading data to iobeam. Otherwise,
the current time is used. If a timestamp is provided in the data,
its granularity (sec,msec,usec) should be specified as a program arg.

Device IDs must be specified in metadata headers if multiple input files
are provided.  If a single input file is provided, device IDs can be
specified either in metadata, from the command line, or the script
will auto-assign.
''')



###############################################################

class ProgramInfo:
    def __init__(self, args):
        self.args = args
        self.files = {}

        # Manage timestamps, either from commend-line or from file metadata
        self.timestampFormat = None
        self.timestampMultiplier = None
        self.timestampSeparation = None
        self.timestampFromColumns = False


class FileInfo:
    def __init__(self, filename):
        self.filename = filename
        # Metadata extracted from input file headers
        self.device_id = None
        self.device_name = None
        self.format = None
        self.formatWithoutTimestamp = None
        self.timestampColumnIndex = -1

        # iobeam objects
        self.iobeamClient = None
        self.iobeamDataStore = None

        # statistics
        self.sent = 0


def returnError(error):
    _parser.print_usage()
    print "\nError: %s" % error
    sys.exit(1)


###############################################################


def addData(progInfo, fileInfo, data, epochTs, cnt):

    if len(fileInfo.format) != len(data):
        returnError(("Number of data elements does not match format\n\tFormat:\t%s\n\tData:\t%s"
                     % (fileInfo.format, data)))

    if fileInfo.timestampColumnIndex < 0:
        thisTs = int(round(epochTs + (cnt * progInfo.timestampSeparation)))
        ts = iobeam.Timestamp(thisTs, unit=progInfo.timestampFormat)
        fileInfo.iobeamDataStore.add(ts, dict(zip(fileInfo.format, data)))
    else:
        thisTs = int(data[fileInfo.timestampColumnIndex])
        ts = iobeam.Timestamp(thisTs, unit=progInfo.timestampFormat)
        del data[fileInfo.timestampColumnIndex]
        fileInfo.iobeamDataStore.add(ts, dict(zip(fileInfo.formatWithoutTimestamp, data)))


def analyzeFiles(progInfo):
    inputFiles = []
    try:
        for fileInfo in progInfo.files.values():
            inputFiles.append((fileInfo, open(fileInfo.filename, 'r')))

    except (OSError, IOError) as e:
        returnError("Problem opening file")


    try:
        addedAny = True
        while addedAny:

            addedAny = False
            epochTs = int(time.time() * progInfo.timestampMultiplier)

            for fileInfo, file in inputFiles:
                cnt = 0
                addedThis = False
                for line in file:
                    line = line.strip()
                    if len(line) == 0 or line[0] == COMMENT_CHAR or line[0] == METADATA_CHAR:
                        continue

                    # Split CSV line into individual values
                    data = map((lambda x: x.strip()), line.split(','))

                    addData(progInfo, fileInfo, data, epochTs, cnt)
                    addedThis = True
                    fileInfo.sent += 1

                    if cnt >= (args.rows_per - 1):
                        break
                    else:
                        cnt += 1


                if addedThis:
                    print "Sent data batch to iobeam: %s" % fileInfo.filename
                    fileInfo.iobeamClient.send()
                    addedAny = True


            time.sleep((progInfo.args.delay_bw / 1000.0))

    except (OSError, IOError) as e:
        returnError("Problem reading file")


###############################################################


def getMetaData(line):
    assert(len(line) > 0 and line[0] == METADATA_CHAR);
    try:
        i = line.index(':')
        key = line[1:i].strip().lower()
        value = line[(i+1):].strip()
        if key == 'columns':
            value =  map((lambda x: x.strip()), value.split(','))
        return (key, value)
    except:
        return None


def extractMetaData(fileInfo):
    try:
        with open(fileInfo.filename, 'r') as file:

            for line in file:
                line = line.strip()
                if len(line) == 0 or line[0] == COMMENT_CHAR:
                    continue
                elif line[0] != METADATA_CHAR:
                    # Read past header
                    break

                metadata = getMetaData(line)

                if metadata == None:
                    returnError("Malformed metadata: %s" % (line))

                key, value = metadata

                if key == 'device_id':
                    fileInfo.device_id = value
                elif key == 'device_name':
                    fileInfo.device_name = value
                elif key == 'columns':
                    fileInfo.format = value

                    fileInfo.formatWithoutTimestamp = list(fileInfo.format)
                    if 'timestamp' in fileInfo.format:
                        fileInfo.timestampColumnIndex = fileInfo.format.index('timestamp')
                        del fileInfo.formatWithoutTimestamp[fileInfo.timestampColumnIndex]

    except (OSError, IOError) as e:
        returnError("Problem accessing file %s" % fileInfo.filename)

    if fileInfo.format == None:
        returnError("Column metadata missing from file %s\n\t! columns: col1,col2,col3,...,colN" % fileInfo.filename)

    if fileInfo.device_name != None and fileInfo.device_id == None:
        returnError("Device ID must be provided if device name specified in file %s" % fileInfo.filename)


def extractAllMetaData(progInfo):

    for filename in progInfo.args.input_file:
        fileInfo = FileInfo(filename)
        progInfo.files[filename] = fileInfo
        extractMetaData(fileInfo)

    # Take device ID from command-line args, but don't want mismatch
    # between device metadata and command-line information
    if progInfo.args.device_id != None:
        assert (len(progInfo.files) == 1)
        fileInfo = progInfo.files[progInfo.args.input_file[0]]
        if (fileInfo.device_id != progInfo.args.device_id) and (fileInfo.device_id != None):
            returnError("Device information supplied does not match file metadata")
        else:
            fileInfo.device_id = progInfo.args.device_id

    if len(progInfo.files) > 1:
        for fileInfo in progInfo.files.values():
            if fileInfo.device_id == None:
                returnError("Device metadata must be present if supplying multiple files")

    for fileInfo in progInfo.files.values():
        if fileInfo.device_id == None and len(progInfo.files) > 1:
            returnError("Device metadata must be present if supplying multiple files")

    # Make sure timestamp information consistent across input files
    countTimestampInMetadata = 0
    for fileInfo in progInfo.files.values():
        if fileInfo.timestampColumnIndex >= 0:
            countTimestampInMetadata += 1

    if countTimestampInMetadata == 0:
        progInfo.timestampFromColumns = False
    elif countTimestampInMetadata == len(progInfo.files):
        progInfo.timestampFromColumns = True
    else:
        returnError("Timestamps must be present in all data files or none")


def configureMetaData(progInfo):

    if progInfo.args.timestamp == 'sec':
        progInfo.timestampFormat = iobeam.TimeUnit.SECONDS
        progInfo.timestampMultiplier = 1
    elif progInfo.args.timestamp == 'msec':
        progInfo.timestampFormat = iobeam.TimeUnit.MILLISECONDS
        progInfo.timestampMultiplier = 1000
    elif progInfo.args.timestamp == 'usec':
        progInfo.timestampFormat = iobeam.TimeUnit.MICROSECONDS
        progInfo.timestampMultiplier = 1000000
    else:
        assert(False)

    # For self-generated timestamps, provide smoothed timestamps over internal.
    # Extra complexity to handle if # rows > delay, and if not using msec for timestamp.
    if not progInfo.timestampFromColumns:
        progInfo.timestampSeparation = float(progInfo.args.delay_bw) / float(progInfo.args.rows_per)
        if progInfo.timestampFormat == iobeam.TimeUnit.SECONDS:
            progInfo.timestampSeparation /= 1000;
        elif progInfo.timestampFormat == iobeam.TimeUnit.MICROSECONDS:
            progInfo.timestampSeparation *= 1000;



###############################################################


def checkArgs(args):

    if not len(args.input_file) > 0:
        returnError("No input files provided")
    if args.device_id != None and len(args.input_file) > 1:
        returnError("If supplying > 1 input file, device info cannot be provided from command-line")
    if args.project_id == None:
        returnError("Unknown project ID")
    if args.token == None:
        returnError("Unknown project token")
    if args.rows_per <= 0:
        returnError("Number of rows must be > 0")
    if args.delay_bw < 0:
        returnError("Delay must be >= 0 milliseconds")
    if args.xmit_count < 0:
        returnError("xmit_count must be >= 0")

    args.timestamp = args.timestamp.lower()
    if not args.timestamp in ['sec', 'msec', 'usec']:
        returnError("Timestamp must be 'sec', 'msec', or 'usec'")



if __name__ == "__main__":

    _parser.add_argument('input_file', nargs='+', help='input file(s)')
    #_parser.add_argument('-i', action='store', dest='input_file', required=True,
    #                    help='input file (required)')
    _parser.add_argument('--pid', action='store', dest='project_id', type=int,
                        help='iobeam project ID', default=IOBEAM_PROJECT_ID)
    _parser.add_argument('--did', action='store', dest='device_id',
                        help='iobeam device ID, auto-generated if not supplied', default=None)
    _parser.add_argument('--token', action='store', dest='token',
                        help='iobeam token', default=IOBEAM_TOKEN)
    _parser.add_argument('--ts', action='store', dest='timestamp',
                        help='timestamp fidelity: sec, msec, usec (default: msec)', default='msec')
    _parser.add_argument('--xmit', action='store', dest='xmit_count', type=int,
                        help='number of times to transmit file (continuously: 0, default: 1)', default=1)
    _parser.add_argument('--rows', action='store', dest='rows_per', type=int,
                        help='rows sent per batch (default: 10)', default=10)
    _parser.add_argument('--delay', action='store', dest='delay_bw', type=int,
                        help='delay in msec between sending data batches (default: 1000)', default=1000)

    args = _parser.parse_args()
    checkArgs(args)

    progInfo = ProgramInfo(args)
    extractAllMetaData(progInfo)
    configureMetaData(progInfo)

    builder = iobeam.ClientBuilder(args.project_id, args.token) \
        .setBackend("https://api.iobeam.com/v1/")

    for fileInfo in progInfo.files.values():
        deviceBuilder = None
        if fileInfo.device_id != None:
            fileInfo.iobeamClient = builder.registerOrSetId(deviceId=fileInfo.device_id,
                                                            deviceName=fileInfo.device_name).build()
            print "Setup device %s [%s]: data format: %s" \
                  % (fileInfo.device_id, fileInfo.device_name, fileInfo.format)
        else:
            fileInfo.iobeamClient = builder.saveToDisk().registerDevice().build()

        fileInfo.iobeamDataStore = fileInfo.iobeamClient.createDataStore(fileInfo.formatWithoutTimestamp)


    repeated = 0
    while args.xmit_count == 0 or repeated < args.xmit_count:
        analyzeFiles(progInfo)
        repeated += 1

    print "\nResults:"
    for fileInfo in progInfo.files.values():
        print "\t%s: %d rows sent" % (fileInfo.filename, fileInfo.sent)
