import yaml

class DataExtractor:

    def read_db_creds(self):
        with open ('db_creds.yaml') as creds_file:
            db_creds_dict = yaml.safe_load(creds_file)
            return db_creds_dict

