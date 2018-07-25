import configuration
from pygresql.pg import DB

def get_objects():
        database = configuration.DATABASE
        con = DB(dbname=database)
        tables = con.query("select schemaname||'.'||tablename as tablename from pg_tables where schemaname='public'")
        tabledict = tables.dictresult()
        tablelist = []
        con.close()
        for dict in tabledict:
                tablelist.append(dict.get('tablename')) # You should replace the 'tablename' with column name from your SQL query in tables variable
        return tablelist

if __name__ == '__main__':
        print(get_objects())
