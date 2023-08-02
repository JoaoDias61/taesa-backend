import pandas as pd

class health_index_all_subsystem:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def health_index_all_subsystem_exec(self):
        query = '''
        SELECT
            e.Descricao AS descricaoEquipamento,
            ie.Descricao AS descricaoInstalacaoEletrica,
            gc.Descricao AS GrupoDeCalculo,
            Calculo.Codigo AS CodigoCalculo,
            Calculo.Descricao,
            MAX(CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)) AS UltimaAtualizacaoCalculo,
            ExecucaoCalculoResultado.Resultado AS HealtIndex
        FROM EngineCalculo.CalculoResultado As ExecucaoCalculoResultado
        INNER JOIN EngineCalculo.CalculoResultadoEquipamento 
            ON CalculoResultadoEquipamento.CalculoId = ExecucaoCalculoResultado.CalculoId 
            AND CalculoResultadoEquipamento.JobId = ExecucaoCalculoResultado.JobId
        INNER JOIN Treetech.ViewEngine_UltimoJob
            ON ViewEngine_UltimoJob.EquipamentoId = CalculoResultadoEquipamento.EquipamentoId
        INNER JOIN EngineCalculo.Calculo
            ON Calculo.Id = ExecucaoCalculoResultado.CalculoId
        INNER JOIN EngineCalculo.CalculoResultadoVariavel As EntradaVariavel 
            ON ExecucaoCalculoResultado.JobId = EntradaVariavel.JobId
        INNER JOIN EngineCalculo.RevisaoVariavel As RevisaoVariavel 
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
            ON f.Id = e.FamiliaId
        INNER JOIN EngineCalculo.CategoriaVariavel AS cv 
            ON cv.Id = Variavel.CategoriaVariavelId
        LEFT JOIN EngineCalculo.GrupoCalculo AS gc 
            ON gc.Id = Calculo.GrupoCalculoId
        WHERE 
            Calculo.Descricao IN (
                'Índice de Estado do Equipamento', -- HI do Equipamento
                'Parte Ativa', -- HI do Subsistema
                'Comutador Sob Carga', -- HI do Subsistema
                'Acessórios', -- HI do Subsistema
                'Ambiente', -- HI do Subsistema
                'Tanque', -- HI do Subsistema
                'Resfriamento', -- HI do Subsistema
                'Bucha', -- HI do Subsistema
                'Preservação do Óleo Isolante' -- HI do Subsistema
            )
            AND e.Id = ?
        GROUP BY
            e.Descricao,
            ie.Descricao,
            gc.Descricao,
            Calculo.Codigo,
            Calculo.Descricao,
            ExecucaoCalculoResultado.Resultado
        ORDER BY MAX(CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE)) DESC
            , ie.Descricao
            , e.Descricao
        '''

        self.cursor.execute(query, self.id_equipment)
        result_sql = self.cursor.fetchall()
        columns = ['descricaoEquipamento', 'descricaoInstalacaoEletrica', 'GrupoDeCalculo', 'CodigoCalculo', 'Familia', 'Descricao', 'UltimaAtualizacaoCalculo', 'HealtIndex']
        data = [dict(zip(columns, row)) for row in result_sql]
        df = pd.DataFrame(data)
        return df
