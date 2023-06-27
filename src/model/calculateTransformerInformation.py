import pandas as pd

class calculateTransformerInformation:
    def __init__(self, cursor, codigo_operacional, familia):
        self.cursor = cursor
        self.codigo_operacional = codigo_operacional
        self.familia = familia

    def executar_calculo(self):

        query = '''
        SELECT DISTINCT
            Calculo.Codigo AS CodigoCalculo
            , ExecucaoCalculoResultado.Resultado AS ResultadoCalculo 
            , EntradaVariavel.UltimaAtualizacao AS UltimaAtualizacaoCalculo
            , ie.Descricao AS descricaoInstalacaoEletrica
            , e.Id AS idEquipamento
            , e.Descricao AS descricaoEquipamento
            , Variavel.Codigo As VariavelCodigo
            , f.Id
            , f.Abreviatura
            , f.Nome

        FROM [EngineCalculo].[CalculoResultado]  As ExecucaoCalculoResultado
            INNER JOIN [EngineCalculo].[CalculoResultadoEquipamento] 
                ON CalculoResultadoEquipamento.CalculoId = ExecucaoCalculoResultado.CalculoId 
                AND CalculoResultadoEquipamento.JobId = ExecucaoCalculoResultado.JobId
            INNER JOIN [Treetech].[ViewEngine_UltimoJob] 
                ON ViewEngine_UltimoJob.EquipamentoId = CalculoResultadoEquipamento.EquipamentoId 
                AND ViewEngine_UltimoJob.JobId = ExecucaoCalculoResultado.JobId
            INNER JOIN [EngineCalculo].[Calculo] 
                ON Calculo.Id = ExecucaoCalculoResultado.CalculoId
            INNER JOIN [EngineCalculo].[CalculoResultadoVariavel] As EntradaVariavel 
                ON ExecucaoCalculoResultado.JobId = EntradaVariavel.JobId
            INNER JOIN [EngineCalculo].[RevisaoVariavel] As RevisaoVariavel 
                ON RevisaoVariavel.VariavelId = EntradaVariavel.VariavelId 
                AND RevisaoVariavel.Revisao = EntradaVariavel.Revisao
            INNER JOIN [EngineCalculo].[Variavel] 
                ON Variavel.Id = EntradaVariavel.VariavelId 
            INNER JOIN Equipamento AS e 
                ON e.Id = CalculoResultadoEquipamento.EquipamentoId
            INNER JOIN EquipamentoInstalacaoEletrica AS eie 
                ON e.Id = eie.EquipamentoId
            INNER JOIN InstalacaoEletrica AS ie 
                ON  ie.Id = eie.InstalacaoEletricaId
            INNER JOIN Familia AS f 
                ON f.TucId = e.TucId
			INNER JOIN EngineCalculo.CategoriaVariavel AS cv 
				ON cv.Id = Variavel.CategoriaVariavelId
        WHERE 1 = 1
            AND f.Nome = 'Autotransformador'
            AND e.CodigoOperacional = '05E3'
            AND Calculo.Codigo IN ('IE_TR_ATV_GP', 'IE_TR_ATV_H2OP', 'REC_MOL_BOLHAS', 'IE_TR_BCH_CAP', 'BCH_CAP_REATOR', 'IE_TR_BCH_TD', 'BCH_TD_REATOR', 'IE_TR_BCH_FUGA')
			-- AND cv.Descricao LIKE '%Parte Ativa%'					
						-- Parte Ativa
						-- Comutador sobcarga
						-- Parte Ativa
						-- Acessórios
						-- Ambiente
						-- Tanque
						-- Resfriamento
						-- Preservação do Óleo Isolante 
						-- Bucha
        ORDER BY ie.Descricao,
			     e.Descricao;
        '''
        self.cursor.execute(query, self.familia, self.codigo_operacional)
        resultado_sql = self.cursor.fetchall()

        colunas = ['CodigoCalculo', 'ResultadoCalculo', 'UltimaAtualizacaoCalculo', 'descricaoInstalacaoEletrica', 'idEquipamento', 'descricaoEquipamento', 'VariavelCodigo', 'Id', 'Abreviatura', 'Nome']

        dados = [dict(zip(colunas, row)) for row in resultado_sql]
        df = pd.DataFrame(dados)

        return df
