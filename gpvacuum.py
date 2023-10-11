import optparse
from multiprocessing import Pool, Value
from gppylib import gplog
from pygresql.pg import DB

# Logger
logger = gplog.get_default_logger()
gplog.setup_tool_logging("gpopb", '', "gpadmin")

database = 'gpadmin'
counter = Value('i', 0)

# Command line option parser
parser = optparse.OptionParser()
parser.add_option('-f', '--file-list', dest='file_list', action='store', help="Specify filename for list of tables")
parser.add_option('-p', '--max-processes', dest='max_processes', action='store', help="Specify maximum number of processes")
parser.add_option('-s', '--schema', dest='schema', action='store', help="Specify schema name")
options, args = parser.parse_args()

def get_objects():
    ''' Get list of objects from file '''
    db_objects = []
    if options.file_list:
        for line in open(options.file_list,'r'):
            db_objects.append(line.rstrip('\n'))
    else:
        con = DB(dbname=database)
        tables = con.query("select schemaname||'.'||tablename as tablename from pg_tables where schemaname='%s'" %options.schema)
        tabledict = tables.dictresult()
        con.close()
        tablelist = []
        for dict in tabledict:
            tablelist.append(dict.get('tablename'))
        db_objects = tablelist
    return db_objects

def run_vacuum(db_object):
    global counter
    ''' Run vacuum on the object '''
    con = DB(dbname = database)
    try:
        con.query("vacuum %s" %(db_object))
    except Exception as e:
        logger.error("Vacuum failed on %s" %(db_object))
        error_list_file.write(db_object + '\n')

    con.close()
    with counter.get_lock():
        counter.value += 1
    logger.info("Vacuum completed on %s: %s" %(db_object, str(counter.value)))

def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args
    
    
if __name__ == '__main__':
    error_list_file = open('error_list.txt','w')
    max_processes = int(options.max_processes)
    db_objects = get_objects()
    pool = Pool(initializer=init, initargs=(counter, ), processes=max_processes)
    pool.map(run_vacuum, db_objects)
    pool.close()
    pool.join()
    logger.info("Object processing completed")
