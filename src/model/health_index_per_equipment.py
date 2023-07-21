import pandas as pd

class health_index_per_equipment:
    def __init__(self, cursor, family):
        self.cursor = cursor
        self.family = family

    def health_index_per_equipment_exec(self):
        query = '''
                SELECT
                    gc.Descricao,
                    Calculo.Codigo AS CodigoCalculo,
                    Calculo.Descricao,
                    MAX(CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)) AS UltimaAtualizacaoCalculo,
                    e.Descricao AS descricaoEquipamento,
                    ie.Descricao AS descricaoInstalacaoEletrica,
                    ExecucaoCalculoResultado.Resultado AS HealtIndex
                FROM [EngineCalculo].[CalculoResultado] AS ExecucaoCalculoResultado
                    INNER JOIN [EngineCalculo].[CalculoResultadoEquipamento]
                        ON CalculoResultadoEquipamento.CalculoId = ExecucaoCalculoResultado.CalculoId
                        AND CalculoResultadoEquipamento.JobId = ExecucaoCalculoResultado.JobId
                    INNER JOIN [Treetech].[ViewEngine_UltimoJob]
                        ON ViewEngine_UltimoJob.EquipamentoId = CalculoResultadoEquipamento.EquipamentoId
                        AND ViewEngine_UltimoJob.JobId = ExecucaoCalculoResultado.JobId
                    INNER JOIN [EngineCalculo].[Calculo]
                        ON Calculo.Id = ExecucaoCalculoResultado.CalculoId
                    INNER JOIN [EngineCalculo].[CalculoResultadoVariavel] AS EntradaVariavel
                        ON ExecucaoCalculoResultado.JobId = EntradaVariavel.JobId
                    INNER JOIN [EngineCalculo].[RevisaoVariavel] AS RevisaoVariavel
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
                        ON f.Id = e.FamiliaId
                    INNER JOIN EngineCalculo.CategoriaVariavel AS cv
                        ON cv.Id = Variavel.CategoriaVariavelId
                    LEFT JOIN EngineCalculo.GrupoCalculo AS gc
                        ON gc.Id = Calculo.GrupoCalculoId
                WHERE
                    1 = 1
                    AND f.Nome = ?
                    AND CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE) = (SELECT MAX(CAST(UltimaAtualizacao AS DATE)) FROM [EngineCalculo].[CalculoResultado])
                    AND gc.Descricao = 'Health Index do Subsistema'
                    AND Calculo.Descricao IN ('Ambiente', 'Bucha', 'Comutador Sob Carga', 'Outros Acessórios', 'Outros Dados', 'Parte Ativa', 'Preservação do Óleo Isolante', 'Resfriamento', 'Tanque Principal')
                GROUP BY
                    gc.Descricao,
                    Calculo.Codigo,
                    Calculo.Descricao,
                    e.Descricao,
                    ie.Descricao,
                    ExecucaoCalculoResultado.Resultado
                ORDER BY
                    UltimaAtualizacaoCalculo DESC,
                    ie.Descricao,
                    e.Descricao
        '''
        self.cursor.execute(query, self.family)
        result_sql = self.cursor.fetchall()

        columns = ['Descricao', 'CodigoCalculo', 'Descricao', 'UltimaAtualizacaoCalculo',
                   'descricaoEquipamento', 'descricaoInstalacaoEletrica', 'HealtIndex']
        data = [dict(zip(columns, row)) for row in result_sql]
        df = pd.DataFrame(data)

        return df
