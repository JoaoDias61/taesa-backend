from reliability.Repairable_systems import optimal_replacement_time
import pandas as pd
import datetime

class calculate_optimal_date:
    def __init__(self, cursor, id_equipment, subsystem, cost_PM, cost_CM):
        self.cursor = cursor
        self.id_equipment = id_equipment
        self.subsystem = subsystem
        self.cost_PM = cost_PM
        self.cost_CM = cost_CM

    def calculate_optimal_date_exec(self):
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

        def taesa_optimal_replacement_time(cost_PM, cost_CM, data_ultima_manut):

            best_interval = optimal_replacement_time(cost_PM=cost_PM, cost_CM=cost_CM, weibull_alpha=1000, weibull_beta=2.5, q=0).ORT
            data_otima = pd.to_datetime(data_ultima_manut) + datetime.timedelta(days=best_interval)

            return data_otima.reset_index(drop=True)
        
        subsystem_data = df[df['Nome'].str.contains(self.subsystem)]
        subsystem_data['dataExecucaoOS'] = pd.to_datetime(subsystem_data['dataExecucaoOS'])

        data = taesa_optimal_replacement_time(self.cost_PM, self.cost_CM, subsystem_data['dataExecucaoOS'])
        
        data_formated = []

        data_dict = data.to_dict()

        if 0 in data_dict:
            data_formated.append({"data_otima": data_dict[0]})
        elif 1 in data_dict:
            data_formated.append({"data_otima": data_dict[1]})

        return data_formated[0]






