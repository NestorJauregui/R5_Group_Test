required_columns = [
'num_documento_cliente', 'tipo_documento_cliente', 'codigo_tienda', 'tipo_tienda', 'fecha_compra', 'total_compra'
]


class Purchases:

    def __init__(self, dataframe, db_client):
        self.data = dataframe
        self.table_name = "purchases"
        self.db_client = db_client
        self.purchases_df = None

    def _check_columns(self):
        assert required_columns in self.data.columns, "The columns are not the required ones"

    def get_clients(self):
        purchases_df = self.data.copy().astype({"tipo_documento_cliente": int, "total_compra": float})
        self.purchases_df = purchases_df[
            ['num_documento_cliente', 'tipo_documento_cliente', 'codigo_tienda', 'tipo_tienda', 'fecha_compra',
             'total_compra']
        ].copy()

    def insert_purchases(self):
        self.db_client.df_to_db(self.purchases_df, self.table_name, 'replace')
        print('La tabla "purchases" se llenó con éxito')

