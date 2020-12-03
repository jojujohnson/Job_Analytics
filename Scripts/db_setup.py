import psycopg2
from db_objects_operations import *

import configparser
config = configparser.ConfigParser()
config.read_file(open('/home/ubuntu/JobAnalytics/Config/config.cfg'))

#Get connection and key values from config file

PSQL_DB          = config.get("POSTGRESQL","PSQL_DB")
PSQL_DB_USER     = config.get("POSTGRESQL","PSQL_DB_USER")
PSQL_DB_PASSWORD = config.get("POSTGRESQL","PSQL_DB_PASSWORD")
PSQL_PORT        = config.get("POSTGRESQL","PSQL_PORT")
PSQL_HOSTNAME    = config.get("POSTGRESQL","PSQL_HOSTNAME")

def create_database():
    """
    - Creates and connects 
    - Returns the connection and cursor 
    """
    
    # connect to default database
    connection = psycopg2.connect(database = PSQL_DB,
                                  user = PSQL_DB_USER,
                                  password = PSQL_DB_PASSWORD,
                                  host = PSQL_HOSTNAME,
                                  port = PSQL_PORT)

    cursor = connection.cursor()

    return cursor, connection


def drop_objects(cursor, connection):
    """
    Drops each objects using the queries list.
    """
    for query in drop_table_queries:
        cursor.execute(query)
        connection.commit()

    print("Table dropped successfully in PostgreSQL")

    for query in drop_sequence_queries:
        cursor.execute(query)
        connection.commit()

    print("Sequence dropped successfully in PostgreSQL")

    for query in drop_index_queries:
        cursor.execute(query)
        connection.commit()

    print("Index dropped successfully in PostgreSQL")


def create_objects(cursor, connection):
    """
    Creates each objects using the queries in list. 
    """
    for query in create_table_queries:
        cursor.execute(query)
        connection.commit()
    
    print("Table created successfully in PostgreSQL")

    for query in create_sequence_queries:
        cursor.execute(query)
        connection.commit()

    print("Sequence created successfully in PostgreSQL")
    
    for query in create_index_queries:
        cursor.execute(query)
        connection.commit()

    print("Index created successfully in PostgreSQL")


def main():
    """
    - Establishes connection with the project database and gets cursor to it.  
    - Drops all the objects.  
    - Creates all objects as needed. 
    - Finally, closes the connection. 
    """
    cursor, connection= create_database()
    
    drop_objects(cursor, connection)
    create_objects(cursor, connection)

    connection.close()

if __name__ == "__main__":
    main()