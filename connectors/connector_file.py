import csv, json

from src.lib.logs.logger import Logger

#from setup import configs


class ConnectorFile:

    def __init__(self, working_dir=''):
        self.working_dir = working_dir

    def read_csv_to_dict(self, file_name):
        file_path = '{}/{}'.format(self.working_dir, file_name)
        with open(file_path, mode='r', encoding="utf-8") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            totalrows = len(rows)

            message = 'Processed {} lines.'.format(totalrows)
            Logger.info(message=message)

            return csv_reader


    def save_dict_to_jsonfile(self, data, file_name):
        file_path = '{}/{}'.format(self.working_dir, file_name)
        try:
            with open(file_path, 'w', encoding="utf-8") as fp:
                json.dump(data, fp)
            return file_path
        except Exception as ex:
            print(ex)
            return None

    def save_dict_to_csvfile(self, data, file_name, fieldnames):
        file_path = '{}/{}'.format(self.working_dir, file_name)
        #file_path = '{}/{}'.format('./static', file_name)

        try:
            with open(file_path, 'w+', encoding="utf-8") as fp:
                fieldnames = fieldnames
                writer = csv.DictWriter(fp, fieldnames=fieldnames)
                writer.writeheader()
                for line in data:
                    writer.writerow(line)
            return file_path
        except Exception as ex:
            print(ex)
            return None

    def save_df_to_csvfile(self, data, file_name, fieldnames):
        file_path = '{}/{}'.format(self.working_dir, file_name)

        try:
            with open(file_path, 'w+', encoding="utf-8") as fp:
                fieldnames = fieldnames
                writer = csv.DictWriter(fp, fieldnames=fieldnames)
                writer.writeheader()
                for c_index, c_row in data.iterrows():
                    data_line = c_row.to_dict()
                    writer.writerow(data_line)
            return file_path
        except Exception as ex:
            print(ex)
            return None


    def get_working_dir(self):
        return self.working_dir





