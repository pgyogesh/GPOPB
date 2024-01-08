import optparse
from multiprocessing import Pool, Value
# from gppylib import gplog
# from pygresql.pg import DB

# Logger
# logger = gplog.get_default_logger()
# gplog.setup_tool_logging("gpopb", '', "gpadmin")

database = 'gpadmin'
counter = Value('i', 0)
error_list_file = 'error_list.txt'

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

def get_table_size(db_object):
    global counter
    con = DB(dbname = database)
    try:
        #dbobject=schema1.table1
        schemaName=db_object.split('.')[0]
        tableName=db_object.split('.')[1]
        query = "INSERT INTO gp_table_sizes VALUES ('%s', '%s', (SELECT pg_total_relation_size('%s.%s')), now())" %(schemaName, tableName, schemaName, tableName)
        con.query(query)
    except Exception as e:
        logger.error("Failed for %s" %(db_object))
        with open(error_list_file, 'a') as f:
            f.write(db_object + '\n')
    con.close()
    with counter.get_lock():
        counter.value += 1
    logger.info("Completed for %s: %s" %(db_object, str(counter.value)))

def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args
    
    
if __name__ == '__main__':
    max_processes = int(options.max_processes)
    db_objects = get_objects()
    pool = Pool(initializer=init, initargs=(counter, ), processes=max_processes)
    pool.map(get_table_size, db_objects)
    pool.close()
    pool.join()
    logger.info("Table sizes are collected")