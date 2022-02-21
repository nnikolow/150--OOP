from etl.extr_tr_load import ETL

x = ETL(ftype="json", location="C://Users/Nikolay.Nikolov2//PycharmProjects//pythonProject9//json_template3", sink='console')

x.source(ftype="file", location="C://Users//Nikolay.Nikolov2//PycharmProjects//pythonProject9//json_template").dsink(sink='postgres')

