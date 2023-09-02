import pandas as pd

class latest_maintenance:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def latest_maintenance_exec(self):
        query = '''
                SELECT 
                	MAX(os.Id) AS NumeroOS
                	, f.Nome
                	, MAX(CAST(os.DataInicialExecucaoReal AS DATE)) AS dataExecucaoOS
                FROM OrdemServico AS os
                INNER JOIN EnsaioOrdemServico AS eos
                	ON eos.OrdemServicoId = os.Id
                INNER JOIN FormularioEnsaio AS fe
                	ON fe.Id = eos.FormularioEnsaioId
                INNER JOIN Formulario AS f
                	ON f.Id = fe.FormularioId
                WHERE 
                	os.EquipamentoId = ?
                	AND f.Codigo IN ('ENR_ATV_DGA_0', 'ENR_BCH_CAP_TD_0')
                GROUP BY
                	f.Nome
            '''
        self.cursor.execute(query, self.id_equipment)
        result_sql = self.cursor.fetchall()

        colunas = [column[0] for column in self.cursor.description]
        data = [dict(zip(colunas, row)) for row in result_sql]
        df = pd.DataFrame(data)
        df['Subsistema'] = df['Nome'].apply(lambda nome: 'Bucha' if 'Bucha' in nome else ('Parte Ativa' if 'Parte Ativa' in nome else None))


        return df
