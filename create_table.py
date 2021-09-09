import csv

from lib import  *
from sql_queries import  *
from cassandra.cluster import Cluster


def execute(session, query):
    try:
        session.execute(query)
    except Exception as e :
        print(f"Failed {e}")

def  exceute_table_create( session, create_table, func):
    # execute  query create table
    for i, item in enumerate(create_table, 1):
        func(session, item)
        print(f'{i}/{len(create_table)} table created')



#using event file
def insert_row( session, query, func, angka):
    file = "event_datefile_new.csv"
    with open (file, 'r', encoding='utf8', newline='') as f :
        csvreader = csv.reader(f)
        next(csvreader)
        for i, line in enumerate(csvreader):

            row_position = func(line)
            session.execute(query,row_position[angka])


def delete_table(session, drop_table , func):
    for a,i in enumerate(drop_table,1):
        func(session, i)
        print(f"{a}/{len(drop_table)} delete processed" )




def main():
    """
    This should connected to  a cassandra  instance your local machine
    """
    try:
        cluster = Cluster(['172.17.0.2'])
        session = cluster.connect()
        print("Connect Estabilished")
    except Exception as e:
        print(f"Connected Failed ! error {e}")

    # Creating Keyspace

    keyspace_query = """
                    CREATE KEYSPACE  IF NOT EXISTS sparkify 
                    WITH REPLICATION = {'class' :'SimpleStrategy', 'replication_factor' : 1}
    """
    execute(session, keyspace_query)
    session.set_keyspace('sparkify')

    delete_table(session,drop_table,execute)
    exceute_table_create(session, create_table, execute)

    for  i, insert in  enumerate(insert_table):
        insert_row(session,insert,kol, i )
    print("Finished processing")

if __name__ =="__main__":

    main()

