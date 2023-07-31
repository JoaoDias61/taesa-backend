import pandas as pd

class risk_matrix_historic:
    def __init__(self, cursor, description):
        self.cursor = cursor
        self.description = description

    def risk_matrix_historic_exec(self):
        query = '''
            SELECT
                MAX(Equipamento.Descricao) AS descricaoEquipamento
                , MAX(InstalacaoEletrica.Descricao) AS descricaoInstalacaoEletrica
                , MAX(gc.Descricao) AS GrupoDeCalculo
                , MAX(Calculo.Codigo) AS CodigoCalculo
                , MAX(Calculo.Descricao) AS DescricaoCalculo
                , ExecucaoCalculoResultado.Resultado 
            	, CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE) AS UltimaAtualizacaoCalculo
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
            INNER JOIN [EngineCalculo].[Variavel] 
            	ON Variavel.Id = EntradaVariavel.VariavelId 
            INNER JOIN Equipamento
            	ON Equipamento.Id = CalculoResultadoEquipamento.EquipamentoId
            INNER JOIN EquipamentoInstalacaoEletrica 
            	ON Equipamento.Id = EquipamentoInstalacaoEletrica.EquipamentoId
            INNER JOIN InstalacaoEletrica
            	ON  InstalacaoEletrica.Id = EquipamentoInstalacaoEletrica.InstalacaoEletricaId
            INNER JOIN Familia AS f 
            	ON f.Id = Equipamento.FamiliaId
            INNER JOIN EngineCalculo.CategoriaVariavel AS cv 
            	ON cv.Id = Variavel.CategoriaVariavelId
            LEFT JOIN EngineCalculo.GrupoCalculo AS gc 
            	ON gc.Id = Calculo.GrupoCalculoId
            WHERE 
            	Calculo.Codigo IN ('IE_TR' ,'I2_TR')
            	AND Equipamento.Descricao = ?
            GROUP BY 
            	ExecucaoCalculoResultado.Resultado 
            	, CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)
            ORDER BY 
            	CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)
                , MAX(InstalacaoEletrica.Descricao)
            	, MAX(Equipamento.Descricao)
                    '''
        self.cursor.execute(query, self.description)
        result_sql = self.cursor.fetchall()

        columns = [
                   'descricaoEquipamento',
                   'descricaoInstalacaoEletrica',
                   'GrupoDeCalculo',
                   'CodigoCalculo',
                   'DescricaoCalculo',
                   'Resultado',
                   'UltimaAtualizacaoCalculo'
                   ]

        data = [dict(zip(columns, row)) for row in result_sql]
        df = pd.DataFrame(data)
        
        return df