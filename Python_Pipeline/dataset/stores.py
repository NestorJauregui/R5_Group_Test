required_columns = ['codigo_tienda', 'tipo_tienda', 'latitud_tienda', 'longitud_tienda', 'id_barrio']


class Stores:

    def __init__(self, dataframe, db_client):
        self.data = dataframe
        self.table_name = "stores"
        self.db_client = db_client
        self.stores_df = None

    def _check_columns(self):
        assert required_columns in self.data.columns, "The columns are not the required ones"

    def get_clients(self):
        stores_df = self.data.groupby(['codigo_tienda', 'tipo_tienda', 'latitud_tienda', 'longitud_tienda',
                                         'id_barrio']).size().reset_index().rename(columns={0: 'count'})
        stores_df['latitud_tienda'] = stores_df['latitud_tienda'].apply(lambda x: x.replace(',', '.'))
        stores_df['longitud_tienda'] = stores_df['longitud_tienda'].apply(lambda x: x.replace(',', '.'))
        stores_df = stores_df.astype({"latitud_tienda": float, "longitud_tienda": float})
        self.stores_df = stores_df[['codigo_tienda', 'tipo_tienda', 'latitud_tienda', 'longitud_tienda', 'id_barrio']].copy()

    def insert_stores(self):
        self.db_client.df_to_db(self.stores_df, self.table_name, 'replace')
        print('La tabla "stores" se llenó con éxito')

