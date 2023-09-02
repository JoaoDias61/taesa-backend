import pandas as pd

class risk_matrix_historic:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def risk_matrix_historic_exec(self):
        query = '''
            SELECT
            	    Equipamento.Descricao AS descricaoEquipamento
            	    , InstalacaoEletrica.Descricao AS descricaoInstalacaoEletrica
            	    , gc.Descricao AS GrupoDeCalculo
            	    , Calculo.Codigo AS CodigoCalculo
            	    , Calculo.Descricao AS DescricaoCalculo
            	    , ExecucaoCalculoResultado.Resultado AS HI
            		, MIN(CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)) AS PeriodoHIInicio
            		, MAX(CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)) AS PeriodoHIFim
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
            	INNER JOIN EquipamentoInstalacaoEletrica AS eie
            		ON eie.EquipamentoId = Equipamento.Id
            	INNER JOIN InstalacaoEletrica AS InstalacaoEletrica
            		ON InstalacaoEletrica.Id = eie.InstalacaoEletricaId
            	INNER JOIN EngineCalculo.GrupoCalculo AS gc
            		ON gc.Id = Calculo.GrupoCalculoId
            	WHERE 
            		Calculo.Codigo IN ('IE_TR' ,'I2_TR')
            	    AND ExecucaoCalculoResultado.UltimaAtualizacao >= '2023-06-16'
            	    AND ExecucaoCalculoResultado.UltimaAtualizacao <= '2023-07-31'
            		AND Equipamento.Id = ?
            	GROUP BY 
            	    Equipamento.Descricao
            	    , InstalacaoEletrica.Descricao
            	    , gc.Descricao
            	    , Calculo.Codigo
            	    , Calculo.Descricao
            	    , ExecucaoCalculoResultado.Resultado
            	ORDER BY 
            		Calculo.Descricao DESC
            		, PeriodoHIFim DESC
            		, PeriodoHIInicio DESC
                    '''
        self.cursor.execute(query, self.id_equipment)
        result_sql = self.cursor.fetchall()

        colunas = [column[0] for column in self.cursor.description]
        data = [dict(zip(colunas, row)) for row in result_sql]

        indice_estado_data = [indice_estado for indice_estado in data if indice_estado['CodigoCalculo'] == "IE_TR"]
        indice_impacto_data = [indice_impacto for indice_impacto in data if indice_impacto['CodigoCalculo'] == "I2_TR"]

        grouped_objects = []

        for indice_estado in indice_estado_data:
            for indice_impacto in indice_impacto_data:
                if indice_estado['PeriodoHIInicio'] >= indice_impacto['PeriodoHIInicio'] and \
                   indice_estado['PeriodoHIFim'] <= indice_impacto['PeriodoHIFim']:
                    grouped_objects.append([
                        indice_estado,
                        indice_impacto
                    ])

        return grouped_objects
