import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn): 
    """
    Load data from s3 to Redshift Staging Tables
    """
    for query in copy_table_queries:
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('{} processed!'.format(query))
    print('All files copied!')


def insert_tables(cur, conn): 
    """
    Copy data from Redshift Staging Tables to Star Schema
    """
    for query in insert_table_queries:
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('{} processed!'.format(query))
    print('All files inserted!')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("AWS Redshift connection established!")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()