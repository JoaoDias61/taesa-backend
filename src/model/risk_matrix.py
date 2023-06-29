import pandas as pd


class risk_matrix:
    def __init__(self, cursor, codigo_operacional, familia):
        self.cursor = cursor
        self.codigo_operacional = codigo_operacional
        self.familia = familia

    def risk_matrix_exec(self):
        query = '''
        SELECT DISTINCT
                      Calculo.Codigo AS CodigoCalculo
                    , ExecucaoCalculoResultado.Resultado AS ResultadoCalculo 
                    , MAX(EntradaVariavel.UltimaAtualizacao) AS UltimaAtualizacaoCalculo
                    , ie.Descricao AS descricaoInstalacaoEletrica
                    , e.Descricao AS descricaoEquipamento
                    , EntradaVariavel.Revisao As RevisaoVariavel 
                    , Variavel.Codigo As VariavelCodigo
                    , Variavel.Descricao As VariavelDescricao
                    , EntradaVariavel.Valor As ResultadoVariavel 
                    , MAX(EntradaVariavel.DataMedicao) AS dataMedicaoVariavel

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
                        ON ie.Id = eie.InstalacaoEletricaId
                    INNER JOIN Familia AS f 
                        ON f.TucId = e.TucId

                WHERE 1 = 1
                    AND f.Nome = ?
                    AND e.Descricao = ?
                    AND RevisaoVariavel.TipoOrigem = 4 --Variágel de origem do ECM
                    AND EntradaVariavel.DataMedicao > '2023-05-29 23:59:59' 
                    AND e.EquipamentoSigmaId is not null 

                GROUP BY Calculo.Codigo, ExecucaoCalculoResultado.Resultado, ie.Descricao, e.Descricao, EntradaVariavel.Revisao, Variavel.Codigo, Variavel.Descricao, EntradaVariavel.Valor

                ORDER BY ie.Descricao,
                        e.Descricao,
                        MAX(EntradaVariavel.DataMedicao) DESC,
                        ExecucaoCalculoResultado.Resultado DESC
        '''
        self.cursor.execute(query, self.familia, self.codigo_operacional)
        resultado_sql = self.cursor.fetchall()

        colunas = ['CodigoCalculo', 'ResultadoCalculo', 'UltimaAtualizacaoCalculo', 'descricaoInstalacaoEletrica',
                   'descricaoEquipamento', 'RevisaoVariavel', 'VariavelCodigo', 'VariavelDescricao', 'ResultadoVariavel', 'dataMedicaoVariavel']


        dados = [dict(zip(colunas, row)) for row in resultado_sql]
        df = pd.DataFrame(dados)

        return df
