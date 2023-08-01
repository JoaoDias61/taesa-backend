import pandas as pd

class filter_data:
    def __init__(self, cursor):
        self.cursor = cursor

    def filter_data_exec(self):
        query = '''
            SELECT 
            	e.Id
            	, e.Descricao
            	, f.Nome
            FROM Equipamento AS e
            INNER JOIN EquipamentoInstalacaoEletrica AS eie 
            	ON e.Id = eie.EquipamentoId
            INNER JOIN InstalacaoEletrica AS ie 
            	ON  ie.Id = eie.InstalacaoEletricaId
            INNER JOIN Familia AS f 
            	ON f.Id = e.FamiliaId
            '''
        resultado = self.cursor.execute(query)
        registro = resultado.fetchall()

        colunas = [column[0] for column in self.cursor.description]
        data = [dict(zip(colunas, row)) for row in registro]
        df = pd.DataFrame(data)

        return df
