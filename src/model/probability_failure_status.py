import pandas as pd
import datetime
import numpy as np
import pandas as pd

class probability_failure_status:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def probability_failure_status_exec(self):

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
            Calculo.Descricao = ('Índice de Estado do Equipamento')
            AND e.Id = ?

                ORDER BY CAST(ExecucaoCalculoResultado.UltimaAtualizacao AS DATE) DESC
                    , ie.Descricao
                	, e.Descricao
        '''

        self.cursor.execute(query, self.id_equipment)        
        result_sql = self.cursor.fetchall()

        columns = ['Descricao', 'descricaoInstalacaoEletrica', 'Descricao', 'CodigoCalculo',
                   'descricaoEquipamento', 'HealtIndex','UltimaAtualizacaoCalculo']
        data = [dict(zip(columns, row)) for row in result_sql]
        df = pd.DataFrame(data)
        df["HealtIndex"] = df["HealtIndex"].astype("int")
        def calculate_probabilities(data, defect_rate = [1e-5*10,
                                                 1e-5*30,
                                                 1e-5*60,
                                                 1e-5*100], 
                                                 failure_rate = 1e-5,
                                                 data_entrada_operação = "01-01-2003"):
                                                 
            # defect rate em dias
            actual_state = data["HealtIndex"].iloc[-1]
            defect_rate = defect_rate[actual_state-1]
        
            data["HealtIndex"] -=  actual_state
            if (data["HealtIndex"] > 0).sum() == 0:
                data_state = data["UltimaAtualizacaoCalculo"].iloc[0]
            else:
                data_state = data[data["HealtIndex"] == 1].iloc[0]["UltimaAtualizacaoCalculo"]
            
            data_diff = (datetime.datetime.today() - pd.to_datetime(data_state)).days
            data_failure_diff = datetime.datetime.today() - pd.to_datetime(data_entrada_operação)
            data_failure_diff = data_failure_diff.days
        
            failure_probability = 1-np.exp((-failure_rate*data_failure_diff))
        
            #FIXME consertar a probabilidade de transição de estado # implementar aqui markov 
            if actual_state == 1:
                state_probability = 1-np.exp((-defect_rate*data_failure_diff)) + failure_probability
            else:
                state_probability = 1-np.exp((-defect_rate*data_diff)) + failure_probability
        
            if state_probability > 1:
                state_probability = 1
            if failure_probability > 1:
                failure_probability = 1
            
            if actual_state == 4:
                failure_probability = state_probability
        
            return {"probabilidade_proximo_estado_equipamento": state_probability,
                    "Probabilidade_proximo_estado_bucha": state_probability*0.3,
                    "Probabilidade_proximo_estado_ativa": state_probability*0.7,
                    "probabilidade_falha_equipamento": failure_probability,
                    "Probabilidade_falha_bucha": failure_probability*0.3,
                    "Probabilidade_falha_ativa": failure_probability*0.7
                    }
        
        result = calculate_probabilities(df)
        
        return result