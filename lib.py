# This script contains helper methods used to manage the Postgres tables

import psycopg2

def connect_sparkify():
    connection = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    connection.set_session(autocommit=True)
    cursor = connection.cursor()
    
    return cursor, connection

def run_query(cursor, query, print_results=True):
    '''
    This method executes the query provided
    '''
    
    cursor.execute(query)
    if print_results:
        rows = cursor.fetchall()
        for r in rows:
            print(r)
            
            
def get_columns(table, cursor=None, connection=None, as_string=True):
    '''
    This returns the column names of the table.
    If as_string is set to True, the column names will be passed as
    an string with the fromat (col1, clo2, ...) to be used in SQL INSERT statements
    '''
    
    if (not cursor) and (not connection):
        cursor, connection = connect_sparkify()
    
    cursor.execute("SELECT * FROM {} LIMIT 0".format(table))
    colnames = [desc[0] for desc in cursor.description]
    
    connection.close()
    
    if as_string:
        return "("+", ".join(map(str, colnames))+")"
    else:
        return colnames

    
def data_format(table):
    """
    This method returns "(%s, %s, ...)" based on the number of colums of the table <table_columns>
    It is useful to avoid hard-coding INSERT INTO statements like:
    
    INSERT INTO <table> (<col1>, <col2>, ...) VALUES (%s, %s, ...)
    """
    
    table_columns = get_columns(table, as_string=False)
    format_columns = ", ".join(map(str, ["%s"]*len(table_columns)))
    
    return "("+format_columns+")"
    

def insert_song_data(filepath,\
                     cursor,\
                     connection,\
                     table):
    
    '''
    This method extracts data from ./data/song_data/ files and insert it into the target <table> 
    Before calling this method take into account that song_id is PRIMARY KEY 
    '''
    
    df = pd.read_json(filepath, lines=True)
    df = df.where(pd.notnull(df), None)
    data_columns = get_columns(cursor=cur, table=table, as_string=False)
    data = df[data_columns].values.tolist()[0]

    data_columns_str = get_columns(cursor=cursor, table=table)
    data_format = ", ".join(map(str, ["%s"]*len(data_columns)))
    data_table_insert = "INSERT INTO {} {} VALUES ({})".format(table,\
                                                               data_columns_str,\
                                                               data_format)
    
    cursor.execute(data_table_insert, data)
    connection.commit()
    
