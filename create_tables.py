import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    #Connects to postgreSQL db and return's the connection and cursor references
    connection = psycopg2.connect("host=127.0.0.1 port=5432 user=postgres password=test")
    connection.set_session(autocommit=True)
    cursor = connection.cursor()
    
    #create the songs_db database
    cursor.execute("DROP DATABASE IF EXISTS songs_db")
    cursor.execute("CREATE DATABASE songs_db WITH ENCODING 'utf8' TEMPLATE template0")

    #close connection to the default database
    connection.close()    

    # connect to the new songs_db database
    connection = psycopg2.connect("host=127.0.0.1 port=5432 dbname=songs_db user=postgres password=test")
    cursor = connection.cursor()

    return cursor, connection

def drop_tables(cursor, connection):
    #execute drop table queries from sql_queries.py
    for query in drop_table_queries:
        cursor.execute(query)
        connection.commit()

def create_tables(cursor, connection):
    #Run's all the create table queries defined in sql_queries.py
    for query in create_table_queries:
        cursor.execute(query)
        connection.commit()


def main():
    cursor, connection = create_database()
    drop_tables(cursor, connection)
    create_tables(cursor, connection)
    connection.close()

if __name__ == "__main__":
    main()