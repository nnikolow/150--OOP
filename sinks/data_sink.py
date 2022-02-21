from etl.extr_tr_load import ETL
import psycopg2




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