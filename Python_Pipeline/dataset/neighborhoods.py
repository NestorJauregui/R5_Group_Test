required_columns = ['id_barrio', 'nombre_barrio']


class Neighborhoods:

    def __init__(self, dataframe, db_client):
        self.data = dataframe
        self.table_name = "neighborhoods"
        self.db_client = db_client
        self.neighborhoods_df = None

    def _check_columns(self):
        assert required_columns in self.data.columns, "The columns are not the required ones"

    def get_clients(self):
        neighborhoods_df = self.data.groupby(['id_barrio', 'nombre_barrio']).size().reset_index().rename(
            columns={0: 'count'})
        self.neighborhoods_df = neighborhoods_df[['id_barrio', 'nombre_barrio']].copy()

    def insert_neighborhoods(self):
        self.db_client.df_to_db(self.neighborhoods_df, self.table_name, 'replace')
        print('La tabla "neighborhoods" se llenó con éxito')

