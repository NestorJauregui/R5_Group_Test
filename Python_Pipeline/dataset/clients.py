required_columns = ['num_documento_cliente', 'tipo_documento_cliente']


class Clients:

    def __init__(self, dataframe, db_client):
        self.data = dataframe
        self.table_name = "slients"
        self.db_client = db_client
        self.clients_df = None

    def _check_columns(self):
        assert required_columns in self.data.columns, "The columns are not the required ones"

    def get_clients(self):
        clients_df = self.data.groupby(
            ['num_documento_cliente', 'tipo_documento_cliente']).size().reset_index().rename(columns={0: 'count'})
        clients_df = clients_df.astype({"tipo_documento_cliente": int})
        self.clients_df = clients_df[['num_documento_cliente', 'tipo_documento_cliente']].copy()

    def insert_clients(self):
        self.db_client.df_to_db(self.clients_df, self.table_name, 'replace')
        print('La tabla "clients" se llenó con éxito')

