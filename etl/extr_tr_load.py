class ETL:
    def __init__(self, ftype, location, sink):
        import psycopg2
        self.ftype = ftype
        self.location = location
        self.sink = sink
        self.text = ''
        self.connection = psycopg2.connect("dbname=db1 user=postgres password=postgres")
        self.cursor = self.connection.cursor()
        self.cursor.execute("set search_path to public")

    def source(self, ftype, location):
        from sources.data_source import DataSource
        if ftype == "file":
            self.text = DataSource.input_data(ftype)
            return self
        elif ftype == "json":
            self.text = DataSource.data_source_json_file(self)
            return self
        else:
            print("source:txt or json")

    def dsink(self, sink):
        from sinks.data_sink import DataSink
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