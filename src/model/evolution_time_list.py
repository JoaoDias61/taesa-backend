from datetime import datetime, timedelta
import pandas as pd

from src.ecm.ECM import ECM

SUBSYSTEM_PARAMS = [
      {"nome": "Capacitância 1", "subsistema": "Bucha", "identificador": 2070, "unidadeMedida": "pF"},
      {"nome": "Capacitância 2", "subsistema": "Bucha", "identificador": 2071, "unidadeMedida": "pF"},
      {"nome": "Capacitância 3", "subsistema": "Bucha","identificador": 2072, "unidadeMedida": "pF"},
      {"nome": "Tendência de evolução da Capacitância 1", "subsistema": "Bucha", "identificador": 2073, "unidadeMedida": "dias"},
      {"nome": "Tendência de evolução da Capacitância 2", "subsistema": "Bucha", "identificador": 2074, "unidadeMedida": "dias"},
      {"nome": "Tendência de evolução da Capacitância 3", "subsistema": "Bucha", "identificador": 2075, "unidadeMedida": "dias"},
      {"nome": "Tangente Delta 1", "subsistema": "Bucha", "identificador": 2076, "unidadeMedida": "%"},
      {"nome": "Tangente Delta 2", "subsistema": "Bucha", "identificador": 2077, "unidadeMedida": "%"},
      {"nome": "Tangente Delta 3", "subsistema": "Bucha", "identificador": 2078, "unidadeMedida": "%"},
      {"nome": "Tendência de evolução da Tangente Delta 1", "subsistema": "Bucha", "identificador": 2079, "unidadeMedida": "%"},
      {"nome": "Tendência de evolução da Tangente Delta 2", "subsistema": "Bucha","identificador": 2080, "unidadeMedida": "%"},
      {"nome": "Tendência de evolução da Tangente Delta 3", "subsistema": "Bucha", "identificador": 2081, "unidadeMedida": "%"},
      {"nome": "Temperatura do Enrolamento 1", "subsistema": "Parte Ativa", "identificador": 2593, "unidadeMedida": "°C"},
      {"nome": "Temperatura do Enrolamento 2", "subsistema": "Parte Ativa","identificador": 2035, "unidadeMedida": "°C"},
      {"nome": "Temperatura do Enrolamento 3", "subsistema": "Parte Ativa","identificador": 2043, "unidadeMedida": "°C"},
      {"nome": "Corrente do enrolamento 1", "subsistema": "Parte Ativa","identificador": 11629, "unidadeMedida": "A"},
      {"nome": "Corrente do enrolamento 2", "subsistema": "Parte Ativa","identificador": 11643, "unidadeMedida": "A"},
      {"nome": "Corrente do enrolamento 3", "subsistema": "Parte Ativa","identificador": 11657, "unidadeMedida": "A"},
      {"nome": "Hidrogênio dissolvido no óleo", "subsistema": "Parte Ativa","identificador": 2363, "unidadeMedida": "ppm"},
      {"nome": "Tendência de evolução do hidrogênio", "subsistema": "Parte Ativa","identificador": 2364, "unidadeMedida": "dias"},
      {"nome": "H2 - Hidrogênio", "subsistema": "Parte Ativa","identificador": 8492, "unidadeMedida": "ppm"},
      {"nome": "CH4 - Metano", "subsistema": "Parte Ativa","identificador": 8493, "unidadeMedida": "ppm"},
      {"nome": "C2H6 - Etano","subsistema": "Parte Ativa", "identificador": 8494, "unidadeMedida": "ppm"},
      {"nome": "C2H4 - Etileno", "subsistema": "Parte Ativa","identificador": 8495, "unidadeMedida": "ppm"},
      {"nome": "C2H2 - Acetileno", "subsistema": "Parte Ativa","identificador": 8496, "unidadeMedida": "ppm"},
      {"nome": "CO - Monóxido de Carbono", "subsistema": "Parte Ativa","identificador": 8497, "unidadeMedida": "ppm"},
      {"nome": "CO2 - Dióxido de Carbono", "subsistema": "Parte Ativa","identificador": 8498, "unidadeMedida": "ppm"},
      {"nome": "N2 - Nitrogênio","subsistema": "Parte Ativa", "identificador": 8499, "unidadeMedida": "ppm"},
      {"nome": "O2 - Oxigênio", "subsistema": "Parte Ativa","identificador": 8500, "unidadeMedida": "ppm"},
      {"nome": "Corrente de Fuga 1", "subsistema": "Bucha","identificador": 2097, "unidadeMedida": "mA"},
      {"nome": "Corrente de Fuga 2", "subsistema": "Bucha","identificador": 2098, "unidadeMedida": "mA"},
      {"nome": "Corrente de Fuga 3", "subsistema": "Bucha","identificador": 2099, "unidadeMedida": "mA"},
      {"nome": "Somatória das Correntes de Fuga - BT (baixa tensão)", "subsistema": "Bucha","identificador": 2187, "unidadeMedida": "mA"},
      {"nome": "Ângulo da Somatória das Correntes - BT (baixa tensão)", "subsistema": "Bucha","identificador": 2209, "unidadeMedida": "°"},
      {"nome": "Ângulo da Somatória das Correntes - MT (média tensão)", "subsistema": "Bucha","identificador": 2823, "unidadeMedida": "°"}
]

class evolution_time_list:
    def __init__(self, cursor, id_equipment, identifier, initial_date, final_date):
        self.cursor = cursor
        self.id_equipment = id_equipment
        self.identifier = identifier
        self.initial_date = initial_date
        self.final_date = final_date

    def extract_identifiers(self, data_params):
        data = []

        for entry in data_params:
            for subsystem in SUBSYSTEM_PARAMS:
                if self.identifier == subsystem['identificador'] and 'valor' in entry and 'Bucha' in subsystem['subsistema']:
                    data.append({
                        "identificador": subsystem['identificador'],
                        "unidadeMedida": subsystem['unidadeMedida'],
                        "subsistema": "Bucha",
                        "valor": entry['valor'],
                        "dataMedicao": entry['data'],
                        "nome": subsystem['nome'],
                    })
                for subsystem in SUBSYSTEM_PARAMS:
                    if self.identifier == subsystem['identificador'] and 'valor' in entry and 'Parte Ativa' in subsystem['subsistema']:
                        data.append({
                            "identificador": subsystem['identificador'],
                            "unidadeMedida": subsystem['unidadeMedida'],
                            "subsistema": "Bucha",
                            "valor": entry['valor'],
                            "dataMedicao": entry['data'],
                            "nome": subsystem['nome'],
                        })

        return data

    def gerar_datas_intervalo(self, data_inicial, data_final):
        data_inicial = datetime.strptime(data_inicial, '%Y-%m-%dT%H:%M:%S')
        data_final = datetime.strptime(data_final, '%Y-%m-%dT%H:%M:%S')

        datas_intervalo = []

        data_atual = data_inicial
        while data_atual <= data_final:
            datas_intervalo.append(data_atual.strftime('%Y-%m-%dT%H:%M:%S'))
            data_atual += timedelta(days=1)

        return datas_intervalo

    def evolution_time_list_exec(self):
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
        
        data_inicial = self.initial_date + 'T00:00:00'
        data_final = self.final_date + 'T00:00:00'

        ecm = ECM()
        data = ecm.request_time_series(data_inicial,
                                    data_final, 
                                    ecm_id,
                                    self.identifier
                                    )
        data_extract = self.extract_identifiers(data)
    
        return data_extract