import pandas as pd

class filter_data:
    def __init__(self, cursor):
        self.cursor = cursor

    def filter_data_exec(self):
        query = '''
        SELECT 
               f.Nome
            ,  e.Descricao
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

        data = [(nome, codigo) for nome, codigo in resultado_sql]

        df = pd.DataFrame(data, columns=['nomeFamilia', 'Descricao'])

        df = df.drop_duplicates().dropna()

        return df
