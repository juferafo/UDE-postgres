# This script contains helper methods used to manage the Postgres tables

def connect_sparkify():
    """
    This method can be used to connect to the "sparkify" database
    """
    
    import psycopg2

    connection = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    connection.set_session(autocommit=True)
    cursor = connection.cursor()
    
    return cursor, connection


def run_query(cursor, query, print_results=True):
    """
    This method can be used to execute a query provided.
    If the parameter print_results is set to True, 
    the output of the query will be printed.
    """
    
    cursor.execute(query)
    if print_results:
        rows = cursor.fetchall()
        for r in rows:
            print(r)
            
            
def get_columns(cursor, table, as_string=True):
    """
    This method returns the column names of a given <table>.
    If the parameter as_string is set to True, the column names will be passed as
    an string with the fromat (col1, clo2, ...) that can be used in SQL-like INSERT statements
    """
            
    cursor.execute("SELECT * FROM {} LIMIT 0".format(table))
    colnames = [desc[0] for desc in cursor.description]
        
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
    

def insert_song_data(filepath, cursor, connection, table):
    """
    This method extracts data from ./data/song_data/ files and insert it into the target <table> 
    Before calling this method take into account that song_id is PRIMARY KEY.
    """
    
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
    
