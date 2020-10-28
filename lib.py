# This script contains helper methods used to manage the Postgres tables

def run_query(cursor, query, print_results=True):
    '''
    This method executes the query provided
    '''
    
    cursor.execute(query)
    if print_results:
        rows = cursor.fetchall()
        for r in rows:
            print(r)
            
            
def get_columns(cursor, table):
    '''
    This returns the column names of the table
    '''
    
    cursor.execute("SELECT * FROM {} LIMIT 0".format(table))
    colnames = [desc[0] for desc in cursor.description]
    return colnames