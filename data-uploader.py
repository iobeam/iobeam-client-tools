import argparse
import time
import sys
import re

from iobeam import iobeam

###############################################################
# Set project ID and token in source code, or from command line
###############################################################

IOBEAM_PROJECT_ID = None
IOBEAM_TOKEN = None

###############################################################

COMMENT_CHAR = '#'
METADATA_CHAR = '!'
BACKEND = "https://api.iobeam.com/v1/"

###############################################################

_parser = argparse.ArgumentParser(version='0.2',
                                 formatter_class=argparse.RawTextHelpFormatter,
                                 description='''
Upload data to iobeam Cloud.

Required input file(s) should be in CSV form, with the header providing
metadata detailing the column names and types, and optionally device
ID/name information:

    # This is a comment
    ! device_id: DEV123
    ! device_name: bored-panda-152
    ! columns: col1, col2 ,col3, ..., colN
    
If one of the columns is \'time\', this integer value is used
as the row\'s timestamp when uploading data to iobeam. Otherwise,
the current time is used. If a time is provided in the data,
its granularity (sec,msec,usec) should be specified as a program arg.

If this time is provided, the uploader can use these times to
determine the delay between transmitting each row (or at some rate
that is faster/slower than the timestamp time). See --xmit-by-time and
--xmit-fast-forward-rate options.

As CSV input does not have type information (compared to JSON, for example),
column types must be specified in header information, as either strings (s),
numbers (n), or booleans (b). Type information should be given in brackets
after the column name, e.g.,

  ! columns: time[n], name[s], temperature[n]

If no type information is provided, a string is assumed:

  ! columns: name, temperature[n]

Column names must be alphanumeric or use special characters ('_', '-').
Names are interpreted in case insensitive fashion. Reserved names 'time',
'time_offset', and 'all' are not allowed.

Device IDs must be specified in metadata headers if multiple input files
are provided.  If a single input file is provided, device IDs can be
specified either in metadata, from the command line, or the script
will auto-assign.
''')



###############################################################

class ColTypes:
    string = 1
    number = 2
    bool = 3

class ProgramInfo:
    def __init__(self, args):
        self.args = args
        self.files = {}

        # Manage timestamps, either from command-line or from file metadata
        self.timeFidelity = None
        self.timeMultiplier = None
        self.timeSeparation = None
        self.timeFromColumns = False

class FileInfo:
    def __init__(self, filename):
        self.filename = filename
        # Metadata extracted from input file headers
        self.device_id = None
        self.device_name = None
        self.format = []
        self.formatTypes = []
        self.formatWithoutTimestamp = []
        self.formatTypesWithoutTimestamp = []
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

def skipRowOrError(skip_invalid, errorMsg):
    if skip_invalid:
        print "Skipping row: %s" % errorMsg
        return None
    else:
        returnError(errorMsg)

def toNumber(s):
    try:
        try:
            return int(s)
        except ValueError:
            return float(s)
    except ValueError:
        return None

def toBool(s):
    s = s.lower()
    if s == 'true' or s == '1':
        return True
    elif s == 'false' or s == '0':
        return False
    else:
        return None

###############################################################


def splitData(line):
    return map((lambda x: x.strip()), line.split(','))


def cleanData(progInfo, fileInfo, rawData):

    if len(fileInfo.format) != len(rawData):
        return skipRowOrError(
            progInfo.args.skip_invalid,
            "Number of columns mismatch in file %s: format %d, row %d" \
                % (fileInfo.filename, len(fileInfo.format), len(rawData)))

    cleanedData = []
    for i in range(0,len(rawData)):
        item = rawData[i]
        if len(item) == 0 or item.lower() == progInfo.args.null_string:
            cleanedData.append(None)
        elif fileInfo.formatTypes[i] == ColTypes.string:
            cleanedData.append(item)
        elif fileInfo.formatTypes[i] == ColTypes.number:
            number = toNumber(item)
            if number == None:
                return skipRowOrError(
                    progInfo.args.skip_invalid,
                    "Column %s data should be numeric: Got [%s] in file %s" \
                        % (fileInfo.format[i], item, fileInfo.filename))
            cleanedData.append(number)
        elif fileInfo.formatTypes[i] == ColTypes.bool:
            bool = toBool(item)
            if bool == None:
                return skipRowOrError(
                    progInfo.args.skip_invalid,
                    "Column %s data should be boolean: Got [%s] in file %s" \
                    % (fileInfo.format[i], item, fileInfo.filename))
            cleanedData.append(bool)
        else:
            raise ValueError("Unknown column type")

    return cleanedData


def addData(progInfo, fileInfo, data, epochTs=0, cnt=0):

    if len(fileInfo.format) != len(data):
        raise Exception("Data cleaning failed: Incorrect number of columns")

    if fileInfo.timestampColumnIndex < 0:
        thisTs = int(round(epochTs + (cnt * progInfo.timeSeparation)))
        ts = iobeam.Timestamp(thisTs, unit=progInfo.timeFidelity)
        fileInfo.iobeamDataStore.add(ts, dict(zip(fileInfo.format, data)))
    else:
        thisTs = data[fileInfo.timestampColumnIndex]
        if type(thisTs) is not int:
            skipRowOrError(
                progInfo.args.skip_invalid,
                "Null time in file %s: %s" % (fileInfo.filename, data))
            return False

        ts = iobeam.Timestamp(thisTs, unit=progInfo.timeFidelity)
        del data[fileInfo.timestampColumnIndex]
        fileInfo.iobeamDataStore.add(ts, dict(zip(fileInfo.formatWithoutTimestamp, data)))

    return True


# Upload delay between data batches accorded to cmd line option
def analyzeFiles(progInfo):
    assert(not progInfo.args.xmit_by_column_time)

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
            epochTs = int(time.time() * progInfo.timeMultiplier)

            for fileInfo, file in inputFiles:
                cnt = 0
                addedThis = False
                for line in file:
                    line = line.strip()
                    if len(line) == 0 or line[0] == COMMENT_CHAR or line[0] == METADATA_CHAR:
                        continue

                    # Split CSV line into individual values
                    cleanedData = cleanData(progInfo, fileInfo, splitData(line))

                    if cleanedData:
                        result = addData(progInfo, fileInfo, cleanedData, epochTs, cnt)
                        if result:
                            addedThis = True
                            fileInfo.sent += 1

                    if cnt >= (args.rows_per - 1):
                        break
                    else:
                        cnt += 1

                if addedThis:
                    print "Sending data batch to iobeam for file %s" % fileInfo.filename
                    fileInfo.iobeamClient.send()
                    addedAny = True


            time.sleep((progInfo.args.delay_bw / 1000.0))

    except (OSError, IOError) as e:
        returnError("Problem reading file")


# Upload delay between data rows accorded to in-file timestamps
def analyzeFileWithIncludedDelay(progInfo):
    assert(progInfo.args.xmit_by_column_time)

    fileInfo = None
    file = None
    try:
        for info in progInfo.files.values():
            fileInfo = info
            file = open(fileInfo.filename, 'r')

    except (OSError, IOError) as e:
        returnError("Problem opening file")

    try:
        line = file.readline()
        nextCleanedData = None

        while line:
            line = line.strip()
            if len(line) == 0 or line[0] == COMMENT_CHAR or line[0] == METADATA_CHAR:
                line = file.readline()
                continue

            if nextCleanedData:
                cleanedData = nextCleanedData
            else:
                # Split CSV line into individual values
                cleanedData = cleanData(progInfo, fileInfo, splitData(line))

            if not cleanedData:
                line = file.readline()
                continue

            thisTime = cleanedData[fileInfo.timestampColumnIndex]
            result = addData(progInfo, fileInfo, cleanedData)
            if result:
                fileInfo.sent += 1
                fileInfo.iobeamClient.send()

            nextLine = file.readline()
            if nextLine:
                # Split CSV line into individual values
                nextCleanedData = cleanData(progInfo, fileInfo, splitData(nextLine))
                nextTime = nextCleanedData[fileInfo.timestampColumnIndex]
                difference = nextTime - thisTime
                if difference > 0:
                    delay = float(difference) / (progInfo.args.xmit_fast_forward_rate * progInfo.timeMultiplier)
                    print "Sent %d rows, pausing %d msec" % (fileInfo.sent, round(delay * 1000))
                    time.sleep(delay)

            line = nextLine

    except (OSError) as e:
        print e
        returnError("Problem reading file")



###############################################################


def getMetaData(line):
    assert(len(line) > 0 and line[0] == METADATA_CHAR);
    try:
        i = line.index(':')
        key = line[1:i].strip().lower()
        value = line[(i+1):].strip()
        if key == 'columns':
            value = map((lambda x: x.strip()), value.split(','))
        return (key, value)
    except:
        return None


def extractFormatAndTypes(fileInfo, metadata):

    for col in metadata:
        m = re.search('^([A-Za-z0-9_\-]+)(\[([A-Za-z]+)\])?', col)
        if not m:
            returnError("Invalid column specification in file %s: %s " % (fileInfo.filename, col))

        colName = m.group(1)
        if colName.lower() == 'time':
            colName = colName.lower()

        fileInfo.format.append(colName)

        colType = m.group(3)
        if not colType:
            fileInfo.formatTypes.append(ColTypes.string)
        else:
            colType = colType.lower()
            if colType == 's':
                fileInfo.formatTypes.append(ColTypes.string)
            elif colType == 'n':
                fileInfo.formatTypes.append(ColTypes.number)
            elif colType == 'b':
                fileInfo.formatTypes.append(ColTypes.bool)
            else:
                returnError("Invalid column type in file %s: %s " % (fileInfo.filename, col))

    fileInfo.formatWithoutTimestamp = list(fileInfo.format)
    fileInfo.formatTypesWithoutTimestamp = list(fileInfo.formatTypes)

    if 'time' in fileInfo.format:
        timestampIndex = fileInfo.format.index('time')
        fileInfo.timestampColumnIndex = timestampIndex

        if fileInfo.formatTypes[timestampIndex] != ColTypes.number:
            returnError("Timestamp column not numeric type in file %s" % fileInfo.filename)

        del fileInfo.formatWithoutTimestamp[timestampIndex]
        del fileInfo.formatTypesWithoutTimestamp[timestampIndex]



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
                    extractFormatAndTypes(fileInfo, value)

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
        progInfo.timeFromColumns = False
    elif countTimestampInMetadata == len(progInfo.files):
        progInfo.timeFromColumns = True
    else:
        returnError("Timestamps must be present in all data files or none")

    if progInfo.args.xmit_by_column_time and not progInfo.timeFromColumns:
        returnError("Transmission by included time requested, but no timestamps present in input file(s)")



def configureMetaData(progInfo):

    if progInfo.args.time_fidelity == 'sec':
        progInfo.timeFidelity = iobeam.TimeUnit.SECONDS
        progInfo.timeMultiplier = 1
    elif progInfo.args.time_fidelity == 'msec':
        progInfo.timeFidelity = iobeam.TimeUnit.MILLISECONDS
        progInfo.timeMultiplier = 1000
    elif progInfo.args.time_fidelity == 'usec':
        progInfo.timeFidelity = iobeam.TimeUnit.MICROSECONDS
        progInfo.timeMultiplier = 1000000
    else:
        assert(False)

    # For self-generated timestamps, provide smoothed timestamps over internal.
    # Extra complexity to handle if # rows > delay, and if not using msec for timestamp.
    if not progInfo.timeFromColumns:
        progInfo.timeSeparation = float(progInfo.args.delay_bw) / float(progInfo.args.rows_per)
        if progInfo.timeFidelity == iobeam.TimeUnit.SECONDS:
            progInfo.timeSeparation /= 1000;
        elif progInfo.timeFidelity == iobeam.TimeUnit.MICROSECONDS:
            progInfo.timeSeparation *= 1000;



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

    args.time_fidelity = args.time_fidelity.lower()
    if not args.time_fidelity in ['sec', 'msec', 'usec']:
        returnError("Time fidelity must be 'sec', 'msec', or 'usec'")

    if args.xmit_by_column_time:
        if args.xmit_fast_forward_rate <= 0.0:
            returnError("Fast forward rate must be > 0")
        if len(args.input_file) != 1:
            returnError("Transmission by column times only supports a single input file")

        args.rows_per = 1
    else:
        if args.xmit_fast_forward_rate != 1.0:
            returnError("Fast forward rate requires --xmit-by-time")

    args.null_string = args.null_string.lower()


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
    _parser.add_argument('--time-fidelity', action='store', dest='time_fidelity',
                        help='time fidelity: sec, msec, usec (default: msec)', default='msec')
    _parser.add_argument('--xmit', action='store', dest='xmit_count', type=int,
                        help='number of times to transmit file (continuously: 0, default: 1)', default=1)
    _parser.add_argument('--rows', action='store', dest='rows_per', type=int,
                        help='rows sent per batch (default: 10)', default=10)
    _parser.add_argument('--delay', action='store', dest='delay_bw', type=int,
                        help='delay in msec between sending data batches (default: 1000)', default=1000)
    _parser.add_argument('--xmit-by-time', action='store_true', dest='xmit_by_column_time',
                         help='delay transmission of successful data rows according to included times')
    _parser.add_argument('--xmit-fast-forward-rate', action='store', dest='xmit_fast_forward_rate', type=float,
                         help='fast forward rate for transmitting data according to timestamp (default: 1.0)',
                         default=1.0)
    _parser.add_argument('--null-string', action='store', dest='null_string',
                         help='case-insensitive string to represent null element (default: null)', default='null')
    _parser.add_argument('--skip-invalid', action='store_true', dest='skip_invalid',
                         help='skip invalid rows from input (otherwise exits with error)')

    _parser.set_defaults(skip_invalid=False)
    _parser.set_defaults(xmit_by_column_time=False)

    args = _parser.parse_args()
    checkArgs(args)

    progInfo = ProgramInfo(args)
    extractAllMetaData(progInfo)
    configureMetaData(progInfo)

    builder = iobeam.ClientBuilder(args.project_id, args.token).setBackend(BACKEND)

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
        if progInfo.args.xmit_by_column_time:
            analyzeFileWithIncludedDelay(progInfo)
        else:
            analyzeFiles(progInfo)
        repeated += 1

    print "\nResults:"
    for fileInfo in progInfo.files.values():
        print "\t%s: %d rows sent" % (fileInfo.filename, fileInfo.sent)
