# Database Name : South German Bank Data
# Keyspace Name : credit
# Table Name : credit_data
import sys
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from src.logger import logging
from src.exception_handler import CustomException
import pandas as pd
import csv

logger = logging.info("Database log")


class dataBaseOperation:

    def __init__(self):

        logging.info('INFO', 'Trying To Connect With The Database')
        self.keyspace = 'credit'

        self.table_name = 'credit_data'

        self.client_id = 'GhZGSNKJojUJTEEbyDdqxUYQ'

        self.client_secret = 'dd5_AhUnWYKZ0-F_gpmd.mJ23cgRIHk3YO0c7NQpnRI.WZ+a.7v5E,c+ZxNu1TU.bb8jHTboa,Qap-JLf5DDxgNrBo8hUw5gmD7iRBUk6JZlSIPiHdRbuZ-+LX-aUOMj'

        self.cloud_config = {
            'secure_connect_bundle': r"G:\AI Projects\Bank_Credit_Risk_Prediction\secure-connect-south-german-bank-data.zip"}

        auth_provider = PlainTextAuthProvider(self.client_id, self.client_secret)
        cluster = Cluster(cloud=self.cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()
        logger.info('INFO', 'The Connection Is Created')

    def usekeyspace(self):

        try:

            logging.info('INFO', 'Using The Keyspace That We Created At Time of Database Creating')
            self.session.execute("USE {keyspace};".format(keyspace=self.keyspace))

            logging.info('INFO', 'The {keyspace} Is Selected'.format(keyspace=self.keyspace))

        except Exception as e:
            raise CustomException(e,sys)

    def createtable(self):

        try:

            logging.info('INFO', 'Table Is Creating Inside The Selected Keyspace')
            self.session.execute("USE {keyspace};".format(keyspace=self.keyspace))

            self.session.execute(
                "CREATE TABLE {table_name}(ID int PRIMARY KEY,status int, duration int,credit_history int,purpose int,"
                "amount int,savings int,employment_duration int,installment_rate int,personal_status_sex int,"
                "other_debtors int,present_residence int,property int, age int, other_installment_plans int,"
                "housing int, number_credits int, job int, people_liable int, telephone int,foreign_worker int,"
                "credit_risk int);".format(table_name=self.table_name))

            logging.info('INFO', 'The Table Is Created Inside The {keyspace} With Name {table_name}'.format(
                        keyspace=self.keyspace, table_name=self.table_name))

        except Exception as e:
            raise CustomException(e,sys)

    def insertintotable(self):

        try:

            logging.info('INFO', 'Inserting The Data Into DATABASE')
            file = "SouthGermanCredit\SouthGermanCredit.csv"
            with open(file, mode='r') as f:
                next(f)

                reader = csv.reader(f, delimiter='\n')
                for i in reader:

                    data = ','.join([value for value in i])
                    self.session.execute("USE {keyspace};".format(keyspace=self.keyspace))

                    self.session.execute(
                        "INSERT INTO {table_name} (ID,status,duration,credit_history,purpose,amount,savings,"
                        "employment_duration,installment_rate,personal_status_sex,other_debtors,present_residence,"
                        "property,age,other_installment_plans,housing,number_credits,job,people_liable,telephone,"
                        "foreign_worker,credit_risk) VALUES ({data});".format(table_name=self.table_name, data=data))

                logging.info('INFO', 'All The Data Entered Into The {keyspace} Having Table Name {table_name}'.
                            format(keyspace=self.keyspace, table_name=self.table_name))

        except Exception as e:
            raise CustomException(e,sys)

    def getdatafromdatabase(self):

        try:

            logging.info('INFO', 'Trying To Get The Data From The DataBase')
            df = pd.DataFrame()

            query = "SELECT * FROM {keyspace}.{table_name};".format(keyspace=self.keyspace, table_name=self.table_name)
            for row in self.session.execute(query):

                df = df.append(pd.DataFrame([row]))

            logging.info('INFO', 'We Gathered The Data From DataBase {}'.format(df))
            
        except Exception as e:
            raise CustomException(e,sys)