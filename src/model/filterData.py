import pandas as pd

class filterData:
    def __init__(self, cursor):
        self.cursor = cursor

    def process_data(self):
        query = '''
        SELECT 
               f.Nome
            ,  e.CodigoOperacional
        FROM Equipamento AS e
        INNER JOIN EquipamentoInstalacaoEletrica AS eie 
            ON e.Id = eie.EquipamentoId
        INNER JOIN InstalacaoEletrica AS ie 
            ON  ie.Id = eie.InstalacaoEletricaId
        INNER JOIN Familia AS f 
            ON f.Id = e.FamiliaId
        '''

        self.cursor.execute(query)
        resultado_sql = self.cursor.fetchall()

        # Extrair os valores individuais de cada tupla
        data = [(nome, codigo) for nome, codigo in resultado_sql]

        df = pd.DataFrame(data, columns=['nomeFamilia', 'codigoOperacional'])
        df['nomeFamilia'] = df['nomeFamilia']
        df['codigoOperacional'] = df['codigoOperacional']

        df = df.drop_duplicates().dropna()

        return df
