import pandas as pd

class health_index_per_subsystem:
    def __init__(self, cursor, initial_date, final_date, family, id_equipment, subsystem=None):
        self.cursor = cursor
        self.id_equipment = id_equipment
        self.family = family
        self.initial_date = initial_date
        self.final_date = final_date
        self.subsystem = subsystem


    def get_calculo_descricao_in(self):
        if self.subsystem is not None:
            return "Calculo.Descricao = (?)"
        else:
            return "Calculo.Descricao = ('Índice de Estado do Equipamento')"

    def health_index_per_subsystem_exec(self):

        query = f'''              
                SELECT DISTINCT
                    e.Descricao AS descricaoEquipamento
                    , ie.Descricao AS descricaoInstalacaoEletrica
                    , gc.Descricao AS GrupoDeCalculo
                    , Calculo.Codigo AS CodigoCalculo
                    , Calculo.Descricao
                    , ExecucaoCalculoResultado.Resultado AS HealtIndex 	
                	, CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE) AS UltimaAtualizacaoCalculo
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
            {self.get_calculo_descricao_in()}
            AND ExecucaoCalculoResultado.UltimaAtualizacao BETWEEN ? AND ?
            AND f.Nome = ?
            AND e.Id = ?

                ORDER BY CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE) DESC
                    , ie.Descricao
                	, e.Descricao
        '''

        if self.subsystem is not None:
            self.cursor.execute(query, self.subsystem, self.initial_date, self.final_date, self.family, self.id_equipment)
        else:
            self.cursor.execute(query, self.initial_date, self.final_date, self.family, self.id_equipment)
        
        result_sql = self.cursor.fetchall()

        columns = ['Descricao', 'descricaoInstalacaoEletrica', 'Descricao', 'CodigoCalculo',
                   'descricaoEquipamento', 'HealtIndex','UltimaAtualizacaoCalculo']
        data = [dict(zip(columns, row)) for row in result_sql]
        df = pd.DataFrame(data)

        if self.subsystem is not None:
            df["subsystem"] = self.subsystem
        
        return df