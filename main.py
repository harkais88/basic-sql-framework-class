# Implementing Basic CRUD operations provided through a single Class
# NOTE: To successfully connect to database, set valid credentials 
# in a new config.ini file or follow instructions below for hard-coded initials

import mysql.connector
import configparser

class Pseudo:

    # Use this constructor for config file input, remove if not to be used
    def __init__(self):
        """
        Class for executing basic CRUD Operations by initializing a database 
        connection and cursor for CRUD operations
        """

        config = configparser.ConfigParser()
        config.read("config.ini")

        self.database = config["mysql"]["database"]

        try:
            self.connection = mysql.connector.connect(
                host = config["mysql"]["host"],
                port = config["mysql"]["port"],
                user = config["mysql"]["user"],
                password = config["mysql"]["password"],
                database = self.database
            )

            print(f" SUCCESFULLY CONNECTED TO DATABASE {self.database}\n")
            self.cursor = self.connection.cursor()

        except mysql.connector.errors.ProgrammingError:
            raise Exception("ERROR: INVALID PARAMETERS PROVIDED, Make Sure correct parameters are given")

    # # # Use this constructor for hard-coded intials, remove if not to be used
    # def __init__(self,host: str,port: int,usr: str,pswd: str,db: str):
    #     """
    #     Class for executing basic CRUD Operations by
    #     initializing a database connection and cursor for CRUD operations
    #     """

    #     self.database = db      
        
    #     try:
    #         self.connection = mysql.connector.connect(
    #             host = host,
    #             port = port,
    #             user = usr,
    #             password = pswd,
    #             database = self.database
    #         )

    #         print(f" SUCCESFULLY CONNECTED TO DATABASE {self.database}\n")
    #         self.cursor = self.connection.cursor()
    #     except mysql.connector.errors.ProgrammingError:
    #         raise Exception("ERROR: INVALID PARAMETERS PROVIDED, Make Sure correct parameters are given")

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

        # Following query specific only to MySQL
        # query = f"SHOW TABLES LIKE '%{table}%'"
        # self.cursor.execute(query)
        
        # if len(self.cursor.fetchall()) == 0:
        #     col_query = ""
        #     for key,value in cols.items(): col_query += f"{key} {value},";
        #     col_query = col_query[:len(col_query)-1]

        #     query = f"CREATE TABLE {table} ({col_query});"
        #     self.cursor.execute(query)
        #     self.connection.commit()
        # else:
        #     print(f"ERROR: TABLE WITH NAME {table} ALREADY EXISTS IN DATABASE {self.database}")

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
            print(f" ERROR: INTEGRITY ERROR DETECTED, POSSIBLE DUPLICATE ENTRY/INVALID COLUMN(S) FOR INPUT {value}")
        except KeyError:
            print(f" ERROR: INVALID COLUMN NAME(S) PROVIDED IN INPUT {value}")
        except mysql.connector.errors.ProgrammingError:
            print(f" ERROR: POSSIBLE SYNTAX ERROR, MAKE SURE TABLE NAME AND VALUES ARE CORRECT")
        print()

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
        print(" CONNECTION SUCCESSFULLY CLOSED")

if __name__ == "__main__":
    #Creating a new object, which sets a connection to a specified database

    #USE THIS IF USING HARD-CODED CREDENTIALS
    #Replace following with valid credentials
    host,port,user,password,database = "localhost",3306,"root","123","arka_db" 
    tester = Pseudo(host,port,user,password,database)
    
    #USE THIS IF USING CONFIG FILE
    # tester = Pseudo()

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

    #Dropping Table
    tester.drop('demo')

    #Closing Connection
    tester.close()
