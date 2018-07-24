from multiprocessing import Pool, Value
from config import configuration
from config import objects
from config import process
from gppylib import gplog
import optparse

# Logger
logger = gplog.get_default_logger()
gplog.setup_tool_logging("gpopb", '', "gpadmin")

# Command line option parser
parser = optparse.OptionParser()
parser.add_option('--ad-hoc', dest='adhoc', action='store', help="Specify filename for list of objects")
options, args = parser.parse_args()

max_processes = configuration.MAX_THREADS

logger.info("Getting list of objects:")
db_objects = []
if options.adhoc:
        for line in open(options.adhoc,'r'):
                db_objects.append(line.rstrip('\n'))
else:
        db_objects = objects.get_objects()

logger.info("Objects to be processed: %s" %',\n'.join(db_objects))

def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args

pool = Pool(initializer=init, initargs=(process.counter, ), processes=max_processes)
pool.map(process.task, db_objects)

pool.close()
pool.join()
logger.info("Object processing completed")
