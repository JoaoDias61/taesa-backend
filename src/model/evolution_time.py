import pandas as pd
import datetime

from src.ecm.ECM import ECM

SUBSYSTEM_PARAMS = {
    "Bucha": [
      {"nome": "Capacitância 1", "identificador": 2070},
      {"nome": "Capacitância 2", "identificador": 2071},
      {"nome": "Capacitância 3", "identificador": 2072},
      {"nome": "Tendência de evolução da Capacitância 1", "identificador": 2073},
      {"nome": "Tendência de evolução da Capacitância 2", "identificador": 2074},
      {"nome": "Tendência de evolução da Capacitância 3", "identificador": 2075},
      {"nome": "Tangente Delta, identificador 1", "identificador": 2076},
      {"nome": "Tangente Delta, identificador 2", "identificador": 2077},
      {"nome": "Tangente Delta, identificador 3", "identificador": 2078},
      {"nome": "Tendência de evolução da Tangente Delta 1", "identificador": 2079},
      {"nome": "Tendência de evolução da Tangente Delta 2", "identificador": 2080},
      {"nome": "Tendência de evolução da Tangente Delta 3", "identificador": 2081},
      {"nome": "Corrente de Fuga, identificador 1", "identificador": 2097},
      {"nome": "Corrente de Fuga, identificador 2", "identificador": 2098},
      {"nome": "Corrente de Fuga, identificador 3", "identificador": 2099},
      {"nome": "Somatória das Correntes de Fuga - BT (baixa tensão)", "identificador": 2187},
      {"nome": "Ângulo da Somatória das Correntes - BT (baixa tensão)", "identificador": 2209},
      {"nome": "Ângulo da Somatória das Correntes - MT (média tensão)", "identificador": 2823}
    ],
    "Parte Ativa": [
      {"nome": "Temperatura do Enrolamento 1", "identificador": 2593},
      {"nome": "Temperatura do Enrolamento 2", "identificador": 2035},
      {"nome": "Temperatura do Enrolamento 3", "identificador": 2043},
      {"nome": "Umidade relativa do óleo", "identificador": 9710},
      {"nome": "Umidade do Papel do Enrolamento 1", "identificador": 11784},
      {"nome": "Umidade do Papel do Enrolamento 2", "identificador": 11787},
      {"nome": "Umidade do Papel do Enrolamento 3", "identificador": 11790},
      {"nome": "Corrente do enrolamento 1", "identificador": 11629},
      {"nome": "Corrente do enrolamento 2", "identificador": 11643},
      {"nome": "Corrente do enrolamento 3", "identificador": 11657},
      {"nome": "Hidrogênio dissolvido no óleo", "identificador": 2363},
      {"nome": "Tendência de evolução do hidrogênio", "identificador": 2364},
      {"nome": "H2 - Hidrogênio", "identificador": 8492},
      {"nome": "CH4 - Metano", "identificador": 8493},
      {"nome": "C2H6 - Etano", "identificador": 8494},
      {"nome": "C2H4 - Etileno", "identificador": 8495},
      {"nome": "C2H2 - Acetileno", "identificador": 8496},
      {"nome": "CO - Monóxido de Carbono", "identificador": 8497},
      {"nome": "CO2 - Dióxido de Carbono", "identificador": 8498},
      {"nome": "N2 - Nitrogênio", "identificador": 8499},
      {"nome": "O2 - Oxigênio", "identificador": 8500}
    ]
  }


class evolution_time:
    def __init__(self, cursor, id_equipment):
        self.cursor = cursor
        self.id_equipment = id_equipment

    def extract_identifiers(self, data_params):
        identifiers = []

        for entry in data_params:
            for typeObject in entry['tipoObjetos']:
                for data_objetos in typeObject['objetos']:
                    for variavel in data_objetos['variaveis']:
                        for bucha_param in SUBSYSTEM_PARAMS['Bucha']:
                            if variavel['identificador'] == bucha_param['identificador'] and 'valor' in variavel:
                                identifiers.append({
                                    "Bucha": {
                                    "identificador": variavel['identificador'],
                                    "codigo": variavel['codigo'],
                                    "valor": variavel['valor'],
                                    "dataMedicao": variavel['dataMedicao'],
                                    "tipoRetorno": variavel['tipoRetorno'],
                                    "nome": bucha_param['nome'],
                                }})
                        for bucha_param in SUBSYSTEM_PARAMS['Parte Ativa']:
                            if variavel['identificador'] == bucha_param['identificador'] and 'valor' in variavel:
                                identifiers.append({
                                    "Parte Ativa": {
                                    "identificador": variavel['identificador'],
                                    "codigo": variavel['codigo'],
                                    "valor": variavel['valor'],
                                    "dataMedicao": variavel['dataMedicao'],
                                    "tipoRetorno": variavel['tipoRetorno'],
                                    "nome": bucha_param['nome'],
                                }})

        return identifiers

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
        data = ecm.request_results(datetime.datetime.now()
                                   .strftime('%Y-%m-%dT%H:%M:%S'), 
                                   datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 
                                   ecm_id)
        data_extract = self.extract_identifiers(data)
        return data_extract
