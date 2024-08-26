# Implementing Basic CRUD operations provided through a single Class
# NOTE: To successfully connect to database, set valid credentials 
# in a new config.ini file or follow instructions below for hard-coded initials

import mysql.connector
import configparser

class Pseudo:

    # Use this constructor for config file input, remove if not to be used
    def __init__(self, host:str = "localhost", port:int = 3306, user:str = "root", 
                 password:str = "Password@123", database:str = "db", use_config = False):
        """
        Class for executing basic CRUD Operations by initializing a database 
        connection and cursor for CRUD operations

        To use hard-coded credentials, set use_config to False and pass valid credentials as parameters to class
        
        To use config file, set use_config to True
        """
        if use_config == True:
            config = configparser.ConfigParser()
            config.read("config.ini")

            try:
                self.database = config["mysql"]["database"]

                self.connection = mysql.connector.connect(
                        host = config["mysql"]["host"],
                        port = config["mysql"]["port"],
                        user = config["mysql"]["user"],
                        password = config["mysql"]["password"],
                        database = self.database
                    )

                print(f" SUCCESFULLY CONNECTED TO DATABASE {self.database}\n")
                self.cursor = self.connection.cursor()

            except KeyError:
                raise Exception("ERROR: Check if your config file is named as 'config.ini', and section under it is given as [mysql]")
            except mysql.connector.errors.ProgrammingError:
                raise Exception("ERROR: INVALID PARAMETERS PROVIDED, Make Sure correct parameters are given")
        else:
            try:
                self.database = database
                
                self.connection = mysql.connector.connect(
                    host = host,
                    port = port,
                    user = user,
                    password = password,
                    database = self.database
                )

                print(f" SUCCESFULLY CONNECTED TO DATABASE {self.database}\n")
                self.cursor = self.connection.cursor()
            except mysql.connector.errors.ProgrammingError:
                raise Exception("ERROR: INVALID PARAMETERS PROVIDED, Make Sure correct parameters are given")            

    def show_table_names(self):
        """Get all table names in given database"""

        query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES             \
                 WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{self.database}'"
        
        self.cursor.execute(query)
        print(f" USING DATABASE {self.database} \n Tables under given database: ")
        print(f" {','.join([head for (head,) in self.cursor.fetchall()])}")
        print()

    def get_headers(self, table: str):
        """Get the Column Names of a given table in a database"""

        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION;"
        self.cursor.execute(query)
        result = [head for (head,) in self.cursor.fetchall()]
        headers = []
        #Removing possible duplicates
        for head in result: 
            if head not in headers: headers.append(head);
        return headers

    def create(self, table: str, cols: dict):
        """Create a table inside database"""

        try:
            col_query = ""
            for key,value in cols.items(): col_query += f"{key} {value},";
            col_query = col_query[:len(col_query)-1]

            query = f"CREATE TABLE {table} ({col_query});"
            self.cursor.execute(query)
            self.connection.commit()

            print(f" TABLE {table}({col_query}) CREATED IN DATABASE {self.database}")
        except mysql.connector.errors.ProgrammingError:
            print(f" ERROR: TABLE WITH NAME {table} ALREADY EXISTS IN DATABASE {self.database}")
        print()

    def drop(self, table: str):
        """Drops a table from database"""

        try:
            query = f"DROP TABLE {table};"
            self.cursor.execute(query)
            self.connection.commit()
            print(f" TABLE {table} REMOVED FROM DATABASE {self.database}")
        except mysql.connector.errors.ProgrammingError:
            print(f" ERROR: TABLE {table} NOT FOUND IN DATABASE {self.database}")
        except mysql.connector.errors.DatabaseError as e:
            print(f" ERROR: {str(e)[str(e).find(':')+1:]}")
        print()

    def insert(self, table: str, values: list[dict]):
        """Inserts a set of values into given table"""

        try:
            headers = self.get_headers(table)

            for value in values: 
                value_query = "("
                for header in headers:
                    if isinstance(value[header],str):
                        value[header] = value[header].replace("'","''")
                        value_query += f"'{value[header]}',";
                    else: value_query += f"{value[header]},";
                value_query = value_query[:len(value_query)-1] + ")"

                query = f"INSERT INTO {table} ({','.join(headers)}) VALUE {value_query};"
                self.cursor.execute(query)
                self.connection.commit()
                print(f" DATA {value_query} ({','.join(headers)}) INSERTED INTO TABLE {table}")

        except mysql.connector.errors.IntegrityError:
            print(f" ERROR: INTEGRITY ERROR DETECTED, POSSIBLE FOREIGN KEY/DUPLICATE ENTRY/INVALID COLUMN(S) FOR INPUT {value}")
        except KeyError:
            print(f" ERROR: INVALID COLUMN NAME(S) PROVIDED IN INPUT {value}")
        except mysql.connector.errors.ProgrammingError:
            print(f" ERROR: POSSIBLE SYNTAX ERROR, MAKE SURE TABLE NAME AND VALUES ARE CORRECT")
        print()

    def delete(self, table: str, values: list[dict]):
        """Deletes rows with matching values"""

        try:
            for value in values:
                del_query_values = " AND ".join([f"{i} = '{j}'" if isinstance(j,str) 
                                                else f"{i} = {j}" for i,j in value.items()])          

                count_query = f"SELECT COUNT(*) FROM {table} WHERE {del_query_values};"
                self.cursor.execute(count_query)
                count = int(self.cursor.fetchall()[0][0])

                del_query = f"DELETE FROM {table} WHERE {del_query_values};"
                self.cursor.execute(del_query)
                self.connection.commit()
                print(f" {count} row(s) matching {del_query_values} deleted from table {table}")
        except mysql.connector.errors.ProgrammingError:
            print(" ERROR: INVALID PARAMETERS PASSED")
        except mysql.connector.errors.IntegrityError as e:
            print(f" ERROR: {str(e)[str(e).find(':')+1:]}")

    def get(self, table: str):
        """Shows the contents of a table in a database"""
        
        try:
            query = f"SELECT * FROM {table};"
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            print(f"\n DISPLAYING TABLE: {table}")
            print(" "+"\t".join(self.get_headers(table)))
            for row in results: print(" "+"\t".join([str(ele) for ele in row]));
        except mysql.connector.errors.ProgrammingError:
            print(f" ERROR: TABLE {table} NOT FOUND IN DATABASE {self.database}")
        print()

    def custom_query(self,query: str):
        """Runs SQL query"""
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            for row in results: print(" " + "\t".join([str(ele) for ele in row]));
        except:
            print("ERROR: INVALID QUERY")

    def close(self):
        """Terminates Database Connection"""

        if self.cursor: self.cursor.close();
        if self.connection: self.connection.close();
        print(f" CONNECTION FOR DATABASE {self.database} SUCCESSFULLY CLOSED")

if __name__ == "__main__":
    #Creating a new object, which sets a connection to a specified database

    #USE THIS IF USING HARD-CODED CREDENTIALS
    #Replace underscores with valid credentials
    # host,port,user,password,database = "_",_,"_","_","_" 
    # tester = Pseudo(host,port,user,password,database)

    #USE THIS IF USING CONFIG FILE
    tester = Pseudo(use_config=True)

    #Shows the tables in specified database
    tester.show_table_names() 

    #Creating a new table
    cols = {"id": "INTEGER NOT NULL", "name": "VARCHAR(50)", "PRIMARY KEY": "(id)"}
    tester.create('demo',cols)

    #Inserting into table
    vals = [{'id': 2, 'name': 'DEF'},{'name': 'ABC', 'id': 1},{'id': 3, 'name': 'GHI'}]
    tester.insert('demo',vals)

    #Testing invalid inputs
    inval1 = [{'id': 3, 'namei': 'GHI'}] #Incorrect column name
    inval2 = [{'id': 3, 'name': 'GHI'}] #Duplicate entry
    inval3 = [{'id': 3, 'name': 'GHI', 'noexist': 'null'}] #Non-existent Column Name
    tester.insert('demo',inval1)
    tester.insert('demo',inval2)
    tester.insert('demo',inval3)
    tester.insert('demo1',inval1) #Passing Non-Existent Table as parameter

    #Displaying Table
    tester.get('demo')

    #Deleting From Table
    del_val = [{'name': 'GHI', 'id': 3}, {'name': 'GHI', 'id': 3}]
    tester.delete('demo',del_val)

    #Displaying Table After Deletion
    print("\n After Deleting Values")
    tester.get('demo')

    #Dropping Table
    tester.drop('demo')

    #Closing Connection
    tester.close()
