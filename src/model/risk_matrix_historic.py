import pandas as pd

class risk_matrix_historic:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def risk_matrix_historic_exec(self):
        query = '''
            SELECT
            	MAX(Equipamento.Descricao) AS descricaoEquipamento
            	, MAX(InstalacaoEletrica.Descricao) AS descricaoInstalacaoEletrica
            	, MAX(GrupoCalculo.Descricao) AS GrupoDeCalculo
            	, MAX(Calculo.Codigo) AS CodigoCalculo
            	, MAX(Calculo.Descricao) AS DescricaoCalculo
            	, ExecucaoCalculoResultado.Resultado AS HI
            	, MIN(CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)) AS DataCalculo
            FROM EngineCalculo.CalculoResultado As ExecucaoCalculoResultado
            INNER JOIN EngineCalculo.CalculoResultadoEquipamento 
            	ON CalculoResultadoEquipamento.CalculoId = ExecucaoCalculoResultado.CalculoId 
            	AND CalculoResultadoEquipamento.JobId = ExecucaoCalculoResultado.JobId
            INNER JOIN EngineCalculo.Calculo
            	ON Calculo.Id = ExecucaoCalculoResultado.CalculoId
            INNER JOIN EngineCalculo.CalculoResultadoVariavel As EntradaVariavel 
            	ON ExecucaoCalculoResultado.JobId = EntradaVariavel.JobId
            INNER JOIN EngineCalculo.RevisaoVariavel As RevisaoVariavel 
            	ON RevisaoVariavel.VariavelId = EntradaVariavel.VariavelId 
            	AND RevisaoVariavel.Revisao = EntradaVariavel.Revisao
            INNER JOIN Equipamento
            	ON Equipamento.Id = CalculoResultadoEquipamento.EquipamentoId
            INNER JOIN EngineCalculo.GrupoCalculo AS GrupoCalculo
            	ON GrupoCalculo.Id = Calculo.GrupoCalculoId
            INNER JOIN EquipamentoInstalacaoEletrica
            	ON EquipamentoInstalacaoEletrica.EquipamentoId = Equipamento.Id
            INNER JOIN InstalacaoEletrica
            	ON InstalacaoEletrica.Id = EquipamentoInstalacaoEletrica.InstalacaoEletricaId
            WHERE 
            	Calculo.Codigo IN ('IE_TR' ,'I2_TR')
            	AND Equipamento.Id = ?
            GROUP BY 
            	ExecucaoCalculoResultado.Resultado
            ORDER BY
            	MIN(CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE))
	            , MAX(Calculo.Codigo)
                    '''
        self.cursor.execute(query, self.id_equipment)
        result_sql = self.cursor.fetchall()

        colunas = [column[0] for column in self.cursor.description]
        data = [dict(zip(colunas, row)) for row in result_sql]
        df = pd.DataFrame(data)

        return df