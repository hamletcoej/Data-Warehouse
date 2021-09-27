import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    this function copies data from S3 into tables stagingsong and stagingevent in redshift database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    this function inserts data into fact and dimension tables from the staging tables copied above
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    this fuction sets up the connection to AWS and executes the above functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #set up connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
            
    #execute functions    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()