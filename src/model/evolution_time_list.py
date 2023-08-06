from datetime import datetime, timedelta
import pandas as pd

from src.ecm.ECM import ECM

SUBSYSTEM_PARAMS = [
      {"nome": "Capacitância 1", "subsistema": "Bucha", "identificador": 2070},
      {"nome": "Capacitância 2", "subsistema": "Bucha", "identificador": 2071},
      {"nome": "Capacitância 3", "subsistema": "Bucha","identificador": 2072},
      {"nome": "Tendência de evolução da Capacitância 1", "subsistema": "Bucha", "identificador": 2073},
      {"nome": "Tendência de evolução da Capacitância 2", "subsistema": "Bucha", "identificador": 2074},
      {"nome": "Tendência de evolução da Capacitância 3", "subsistema": "Bucha", "identificador": 2075},
      {"nome": "Tangente Delta, identificador 1", "subsistema": "Bucha", "identificador": 2076},
      {"nome": "Tangente Delta, identificador 2", "subsistema": "Bucha", "identificador": 2077},
      {"nome": "Tangente Delta, identificador 3", "subsistema": "Bucha", "identificador": 2078},
      {"nome": "Tendência de evolução da Tangente Delta 1", "subsistema": "Bucha", "identificador": 2079},
      {"nome": "Tendência de evolução da Tangente Delta 2", "subsistema": "Bucha","identificador": 2080},
      {"nome": "Tendência de evolução da Tangente Delta 3", "subsistema": "Bucha", "identificador": 2081},
      {"nome": "Corrente de Fuga, identificador 1", "subsistema": "Bucha","identificador": 2097},
      {"nome": "Corrente de Fuga, identificador 2", "subsistema": "Bucha","identificador": 2098},
      {"nome": "Corrente de Fuga, identificador 3", "subsistema": "Bucha","identificador": 2099},
      {"nome": "Somatória das Correntes de Fuga - BT (baixa tensão)", "subsistema": "Bucha","identificador": 2187},
      {"nome": "Ângulo da Somatória das Correntes - BT (baixa tensão)", "subsistema": "Bucha","identificador": 2209},
      {"nome": "Ângulo da Somatória das Correntes - MT (média tensão)", "subsistema": "Bucha","identificador": 2823},
      {"nome": "Temperatura do Enrolamento 1", "subsistema": "Parte Ativa", "identificador": 2593},
      {"nome": "Temperatura do Enrolamento 2", "subsistema": "Parte Ativa","identificador": 2035},
      {"nome": "Temperatura do Enrolamento 3", "subsistema": "Parte Ativa","identificador": 2043},
      {"nome": "Umidade relativa do óleo", "subsistema": "Parte Ativa","identificador": 9710},
      {"nome": "Umidade do Papel do Enrolamento 1", "subsistema": "Parte Ativa","identificador": 11784},
      {"nome": "Umidade do Papel do Enrolamento 2", "subsistema": "Parte Ativa","identificador": 11787},
      {"nome": "Umidade do Papel do Enrolamento 3", "subsistema": "Parte Ativa","identificador": 11790},
      {"nome": "Corrente do enrolamento 1", "subsistema": "Parte Ativa","identificador": 11629},
      {"nome": "Corrente do enrolamento 2", "subsistema": "Parte Ativa","identificador": 11643},
      {"nome": "Corrente do enrolamento 3", "subsistema": "Parte Ativa","identificador": 11657},
      {"nome": "Hidrogênio dissolvido no óleo", "subsistema": "Parte Ativa","identificador": 2363},
      {"nome": "Tendência de evolução do hidrogênio", "subsistema": "Parte Ativa","identificador": 2364},
      {"nome": "H2 - Hidrogênio", "subsistema": "Parte Ativa","identificador": 8492},
      {"nome": "CH4 - Metano", "subsistema": "Parte Ativa","identificador": 8493},
      {"nome": "C2H6 - Etano","subsistema": "Parte Ativa", "identificador": 8494},
      {"nome": "C2H4 - Etileno", "subsistema": "Parte Ativa","identificador": 8495},
      {"nome": "C2H2 - Acetileno", "subsistema": "Parte Ativa","identificador": 8496},
      {"nome": "CO - Monóxido de Carbono", "subsistema": "Parte Ativa","identificador": 8497},
      {"nome": "CO2 - Dióxido de Carbono", "subsistema": "Parte Ativa","identificador": 8498},
      {"nome": "N2 - Nitrogênio","subsistema": "Parte Ativa", "identificador": 8499},
      {"nome": "O2 - Oxigênio", "subsistema": "Parte Ativa","identificador": 8500}
  ]

class evolution_time_list:
    def __init__(self, cursor, id_equipment, identifier, initial_date, final_date):
        self.cursor = cursor
        self.id_equipment = id_equipment
        self.identifier = identifier
        self.initial_date = initial_date
        self.final_date = final_date


    def extract_identifiers(self, data_params):
        identifiers = []
        teste = []
        print(data_params)

        for entry in data_params:
            for typeObject in entry['tipoObjetos']:
                for data_objetos in typeObject['objetos']:
                    for variavel in data_objetos['variaveis']:
                        for subsystem in SUBSYSTEM_PARAMS:
                            if variavel['identificador'] == subsystem['identificador'] and 'valor' in variavel and 'Bucha' in subsystem['subsistema']:
                                teste.append({
                                    "identificador": variavel['identificador'],
                                    "codigo": variavel['codigo'],
                                    "subsistema": "Bucha",
                                    "valor": variavel['valor'],
                                    "dataMedicao": variavel['dataMedicao'],
                                    "tipoRetorno": variavel['tipoRetorno'],
                                    "nome": subsystem['nome'],
                                })
                            elif variavel['identificador'] == subsystem['identificador'] and 'valor' in variavel and 'Parte Ativa' in subsystem['subsistema']:
                                identifiers.append({
                                            "identificador": variavel['identificador'],
                                            "codigo": variavel['codigo'],
                                            "subsistema": "Parte Ativa",
                                            "valor": variavel['valor'],
                                            "dataMedicao": variavel['dataMedicao'],
                                            "tipoRetorno": variavel['tipoRetorno'],
                                            "nome": subsystem['nome'],
                                        })

        return identifiers

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

        intervalo_de_datas = self.gerar_datas_intervalo(data_inicial, data_final)

        ecm = ECM()

        ecm_list = []
        for time in intervalo_de_datas:
            data = ecm.request_results(time,
                                        time, 
                                        ecm_id)
            data_extract = self.extract_identifiers(data)
            ecm_list.append(data_extract[0])
        
        filter_data = [objeto for objeto in ecm_list if objeto["identificador"] == self.identifier]

        return filter_data
