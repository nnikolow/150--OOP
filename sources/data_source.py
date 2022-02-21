from etl.extr_tr_load import ETL
from faker import Faker
import random
import string
import json



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