import configparser
import psycopg2
import sql_queries as sql_q


def create_tables(cur, conn):
    """
    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    """
    for q in sql_q.create_table_queries:
        print('executing query: {}'.format(q))
        cur.execute(q)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into star schema DB from staging tables.
    cur and conn and the curson and connection from the psycopg2 API to the redshift DB.
    """
    for query in insert_table_queries:
        print('executing query: {}'.format(query))
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to redshift DB, loads data into staging tables, and runs ETL to put data in a star schema.
    """
    config = configparser.ConfigParser()
    # should be connection_filename from infrastructure_as_code.py
    config_file = os.path.userexpand('~/.aws_creds/solar_cluster.cfg')
    config.read(config_file)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()