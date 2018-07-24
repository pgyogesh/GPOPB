from multiprocessing import Value
from pygresql.pg import DB
from gppylib import gplog
import configuration
logger = gplog.get_default_logger()
gplog.setup_tool_logging("gpopb", '', "gpadmin")

database = configuration.DATABASE
counter = Value('i', 0)
def task(db_object):
        global counter
        con = DB(dbname = database)
        con.query("vacuum %s" %(db_object))
        con.close()
        with counter.get_lock():
                counter.value += 1
        logger.info(str(counter.value) + " objects completed")

if __name__ == '__main__':
        print("this program should not be running alone :P")
