from database.redshift import RedshiftServerless
import os


class DataBase:
    def __init__(
            self,
            database_source="redshift",
            endpoint=None,
            user=None,
            password=None
    ):

        self.endpoint = endpoint
        self.user = user
        self.password = password
        self._get_aws_credentials()

        if database_source == "redshift":
            self.engine = RedshiftServerless(
                aws_access_key=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
                endpoint=self.endpoint,
                user=self.user,
                password=self.password
            )

    def connect(self):
        self.engine.connect()

    def disconnect(self):
        self.engine.disconnect()

    def execute_query(self, query):
        return self.engine.execute_query(query)

    def query_to_df(self, query):
        return self.engine.query_to_df(query)

    def df_to_db(self, dataframe, table_name, method="append"):
        self.engine.df_to_db(dataframe, table_name, method=method)

    def _get_aws_credentials(self):
        self.aws_access_key = os.environ.get("AWS_ACCESS_KEY")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


