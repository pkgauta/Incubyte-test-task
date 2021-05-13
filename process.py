import csv
import sqlite3


filename = 'input.csv'
dbname = 'test.db'


def initialize_db():
    conn = sqlite3.connect(dbname)  # creates db connection
    return conn


def close_db( conn):
    conn.close()   # close connection from db
    return True


def create_table( conn, table_name):
    query = f'''CREATE TABLE IF NOT EXISTS {table_name}
             (
             Customer_Name           CHAR(255)       NOT NULL,
             Customer_Id             CHAR(18)        NOT NULL,
             Open_Date      DATE            NOT NULL,
             Last_Consulted_Date     DATE,
             Vaccination_Id        CHAR(5),           
             Dr_Name        CHAR(255),         
             State                   CHAR(5),            
             Country                 CHAR(5),
             DOB                     DATE,
             Is_Active         CHAR(1)
             );'''
    conn.execute(query)  # create table into tha database
    print("new table created")
    return True


def read_table_data( conn, table_name, where_clause=None):
    query = f"SELECT * FROM {table_name}"
    _query = f'''SELECT * FROM {table_name} WHERE {where_clause} COLLATE NOCASE'''
    resp = conn.execute(query if not where_clause else _query)  # read data with or without where clause.
    return [row for row in resp]   # return list of response from cursor object


def insert_data( conn, **kwargs):
    query = f"INSERT INTO {kwargs['tb_name']} VALUES {kwargs['placeholders']}"
    conn.execute(query)  # insert data to specific table
    return True


def check_existing_table( conn, table_name):
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    resp = conn.execute(query)  # check if table exists in the database
    return bool([row for row in resp])


def parse_csv():
    with open(filename, 'r') as file:
        csv_file = csv.DictReader(file, delimiter="|")
        for row in csv_file:
            resp = dict(row)
            break
    return list(resp.keys())


def perform_action():
    if not filename.endswith('.csv'):
        return "Incorrect file format"

    countries_list = []
    cur = initialize_db()
    create_table(cur, 'Staging')
    with open(filename, 'r') as file:
        csv_file = csv.DictReader(file, delimiter="|")
        for row in csv_file:
            resp = dict(row)
            if '' in resp:
                resp.pop('')
                resp.pop('H')

            insert_data(conn=cur, tb_name='Staging', placeholders=tuple(resp.values()))
            countries_list.append(resp["Country"].lower())
        cur.commit()
        countries_list = list(set(countries_list))
        for country in countries_list:
            resp = read_table_data(cur, 'Staging', f'Country="{country}"')
            if not check_existing_table(cur, country):
                create_table(conn=cur, table_name=country)
                cur.commit()
            final_response = [insert_data(cur, tb_name=country, placeholders=data) for data in resp]
        cur.commit()
        close_db(cur)
        if all(final_response):
            return "Operation performed successfully"
        else:
            return "Something bad happened"


response = perform_action()
print(response)