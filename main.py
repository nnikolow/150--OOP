import json
import sys
from faker import Faker
import random
import string
import psycopg2


class ETL:
    def __init__(self, ftype, location, sink):
        self.ftype = ftype
        self.location = location
        self.sink = sink
        self.text = ''
        self.connection = psycopg2.connect("dbname=db1 user=postgres password=postgres")
        self.cursor = self.connection.cursor()
        self.cursor.execute("set search_path to public")


    def source(self, ftype, location):
        if ftype == "file":
            self.text = DataSource.input_data(ftype)
            return self
        elif ftype == "json":
            self.text = DataSource.data_source_json_file(self)
            return self
        else:
            print("source:txt or json")

    def dsink(self, sink):
        if sink == "console":
            #.sink = DataSource.DataSink.console(self.sink)
            #return self
            return DataSink.console(self)
        elif sink == "postgres":
            #self.sink = DataSource.DataSink.postgres(self.sink)
            #return self
            return DataSink.postgres(self)
        else:
            print("sink: console or postgres")


class DataSource(ETL):
    def input_data(self):
        fake = Faker()
        def rand_key():

            num = str(random.randint(0, 10))
            characters = ''.join(random.choice(string.ascii_uppercase) for i in range(3))
            result = num + characters
            return result

        rand_data = {'key': rand_key(),
                     'value': round(random.uniform(0, 50), 1),
                     'ts': str(fake.date_time())}

        with open('rand_data.json', 'w') as fp:
            json.dump(rand_data, fp)
        return rand_data


    def data_source_json_file(self):
        def json_file():
            with open(self.location, encoding='utf-8', errors='ignore') as json_data:
                data = json.load(json_data)
                return data

        # def print_to_stdout(*a):
        #     print(*a, file=sys.stdout)

        k = json_file()
        return k

class DataSink(ETL):
    def __init__(self, ftype, location, sink):
        super().__init__(ftype, location, sink)
        self.ftype = ftype
        self.connection = psycopg2.connect("dbname=db1 user=postgres password=postgres")
        self.cursor = self.connection.cursor()
        self.cursor.execute("set search_path to public")

    def console(self):
        return self.text

    def postgres(self):
        try:
            print("Opening file...\n")
            with open(self.location) as file:
                data = file.read()

            query_sql = """
            insert into table1 select * from
            json_populate_recordset(NULL::table1, %s);
            """

            self.cursor.execute(query_sql, (data,))
            self.connection.commit()
            print("File successfully loaded! \n")
        except psycopg2.errors.InvalidParameterValue:
            print("Error in the JSON file -> reformat the file and try again.")


'''
Execution Logic
'''

x = ETL(ftype="json", location="C://Users/Nikolay.Nikolov2//PycharmProjects//pythonProject9//json_template3", sink='console')
#print(type(x))
# print(x.ftype, x.location, x.sink)
print(x.source(ftype="file", location="C://Users//Nikolay.Nikolov2//PycharmProjects//pythonProject9//json_template").dsink(sink='postgres'))

# print(x.dsink(sink='console'))
