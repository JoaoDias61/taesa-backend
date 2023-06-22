import pandas as pd

class DataProcessor:
    def __init__(self, cursor):
        self.cursor = cursor

    def process_data(self):
        query = '''
        SELECT
            f.Nome AS nomeFamilia,
            e.CodigoOperacional
        FROM
            Equipamento AS e
        LEFT JOIN
            Familia AS f ON f.TucId = e.TucId
        ORDER BY
            f.Id, e.Id;
        '''

        self.cursor.execute(query)
        resultado_sql = self.cursor.fetchall()

        # Extrair os valores individuais de cada tupla
        data = [(nome, codigo) for nome, codigo in resultado_sql]

        df = pd.DataFrame(data, columns=['nomeFamilia', 'codigoOperacional'])
        df['nomeFamilia'] = df['nomeFamilia'].str.upper()
        df['codigoOperacional'] = df['codigoOperacional'].str.upper()

        df = df.drop_duplicates().dropna()

        return df
