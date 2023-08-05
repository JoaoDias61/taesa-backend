import pandas as pd
import datetime

from src.ecm.ECM import ECM

class evolution_time:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def evolution_time_exec(self):
        query = '''
            SELECT e.Id, e.Descricao, e.EquipamentoSigmaId FROM Equipamento AS e
            WHERE 
                e.EquipamentoSigmaId is not null
                AND e.Id = ?
                '''
        self.cursor.execute(query, self.id_equipment)
        result_sql = self.cursor.fetchall()

        colunas = [column[0] for column in self.cursor.description]
        data = [dict(zip(colunas, row)) for row in result_sql]
        df = pd.DataFrame(data)
        ecm_id = int(df.EquipamentoSigmaId.values[0])
        ecm = ECM()
        data = ecm.request_results(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), ecm_id)
        print(data)
        return data
