import json

import pandas as pd
from sqlalchemy import create_engine
import boto3

from pandas import DataFrame


class RedshiftServerless:
    def __init__(
            self, aws_access_key, aws_secret_access_key, endpoint=None, user=None, password=None
    ):
        self.endpoint = endpoint
        self.user = user
        self.password = password
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.secret_name = "redshift_cluster_access"
        self.aws_region_name = "us-east-1"
        self.engine = None

        if self.aws_access_key is not None and self.aws_secret_access_key is not None:
            self._set_db_credentials()

    def connect(self):
        try:
            conn_str = f"postgresql://{self.user}:{self.password}@{self.endpoint}"
            self.engine = create_engine(conn_str)
            print("Connected to Redshift serverless successfully!")
        except Exception as e:
            print("Error connecting to Redshift serverless:", e)

    def disconnect(self):
        if self.engine is not None:
            self.engine.dispose()
            print("Disconnected from Redshift serverless.")

    def execute_query(self, query):
        if self.engine is None:
            print("Not connected to Redshift db. Please connect")
            return

        try:
            with self.engine.connect() as connection:
                result = connection.execute(query)
                rows = result.fetchall()
                return rows
        except Exception as e:
            print("Error executing query:", e)

    def df_to_db(self, dataframe: DataFrame, table_name, method="append"):
        if self.engine is None:
            print("Not connected to Redshift db. Please connect")
            return
        dataframe.to_sql(
            name=table_name,
            con=self.engine,
            index=False,
            if_exists=method
        )

    def query_to_df(self, query):
        if self.engine is None:
            print("Not connected to Redshift db. Please connect")
            return
        return pd.read_sql_query(query, self.engine)

    def _set_db_credentials(self):

        if self.aws_access_key is None or self.aws_secret_access_key is None:
            client = boto3.client(
                service_name='secretsmanager',
                region_name=self.aws_region_name,
            )
        else:
            client = boto3.client(
                service_name='secretsmanager',
                region_name=self.aws_region_name,
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
            )

        get_secret_value_response = client.get_secret_value(
            SecretId=self.secret_name
        )

        # Decrypts secret using the associated KMS key.
        secret = json.loads(get_secret_value_response['SecretString'])
        self.endpoint = secret.get("host")
        self.user = secret.get("user")
        self.password = secret.get("password")


