import requests
import json
from timeit import default_timer as timer
from datetime import *


class ECM:
    url_homolog = "https://d57b-179-111-23-20.ngrok-free.app/"
    url_prod = "http://172.21.184.6:8088/"
    url_base = url_homolog
    url_req_variables = url_base + "api/eam/obter/variaveis"
    url_req_results = url_base + "api/eam/obter/dadosvariaveis"

    variables_ecm = [
        2006,
        2001,
        2070,
        2071,
        2072,
        2073,
        2074,
        2075,
        2076,
        2077,
        2078,
        2079,
        2080,
        2081,
        2593,
        2035,
        2043,
        11629,
        11643,
        11657,
        2363,
        2364,
        8492,
        8493,
        8494,
        8495,
        8496,
        8497,
        8498,
        8499,
        8500,
        2097,
        2098,
        2099,
        2187,
        2209,
        2823
    ]

    @staticmethod
    def format_date_to_req(data_inicial):
        return datetime.strptime(data_inicial, '%Y-%m-%dT%H:%M:%S')

    @staticmethod
    def generateDates(data_inicial):
        return datetime.strptime(data_inicial, '%Y-%m-%dT%H:%M:%S')

    def request_most_recent(self, dt: any, ecm_id: int, ecm_var_id: int) -> list[dict]:
        json_req = self.build_request(dt, dt, ecm_id, ecm_var_id, 1)
        headers = {
            'Content-Type': 'application/json'
        }

        res = requests.post(
            url=ECM.url_req_results,
            data=json.dumps(json_req),
            headers=headers
        )

        return res.json()

    def request_time_series(self, start_dt: any, end_dt: any, ecm_id: int, ecm_var_id: int) -> list[dict]:
        tic = timer()

        json_req = self.build_request(start_dt, end_dt, ecm_id, ecm_var_id, 64)
        headers = {
            'Content-Type': 'application/json'
        }

        res = requests.post(
            url=ECM.url_req_results,
            data=json.dumps(json_req),
            headers=headers
        )
        time_to_req = float(timer() - tic)

        #print(f'Address: {self.url_base}; Equip: {ecm_id}; Date: {start_dt}; Result: {res}. Took {time_to_req:.2f} seconds..')

        return self.get_time_series(res.json())

    @staticmethod
    def get_time_series(response: list[dict]) -> list[dict]:
        variaveis = response[0]['tipoObjetos'][0]['objetos'][0]['variaveis']
        time_series = []
        for variavel in variaveis:
            time_series.append(
                {
                    'valor': variavel['valor'],
                    'data': variavel['dataMedicao']
                }
            )

        return time_series

    @staticmethod
    def filter_nan(response: list[dict]) -> list[dict]:
        for tipo in response[0]["tipoObjetos"]:
            for obj in tipo['objetos']:
                for k in obj['variaveis']:
                    if 'valor' in k:
                        if k['valor'] == 'NaN' or k['valor'] == '':
                            k.pop('valor')

        return response

    def build_request(self, start_dt: any, end_dt: any, ecm_id: int, ecm_var_id: int, tipoRetorno: int) -> list[dict]:
        ecm_variables = self.request_variables()

        req_body = [
            {
                "equipamentoSigmaId": ecm_id
            }
        ]

        tipo_obj = []
        for tipo in ecm_variables:
            # tipo : int
            # objetos: []
            all_objects = []
            tem_var = 0
            for objeto in tipo['objetos']:
                var_list = []
                new_objetcs = {}

                for variavel in objeto['variaveis']:
                    if variavel['identificador'] == ecm_var_id:
                        # FILTRAR AQUI OS CAMPOS DA VARIAVEL
                        #print(f' var {variavel} -> Tipo: {tipo["tipoObjeto"]} Modulo: {objeto["identificador"]}/{objeto["codigo"]}')
                        vaux = variavel
                        vaux.pop('descricao')
                        vaux['dataInicio'] = start_dt
                        vaux['dataFim'] = end_dt
                        vaux['tipoRetorno'] = tipoRetorno
                        var_list.append(vaux)

                if len(var_list) > 0:
                    tem_var = 1
                    new_objetcs['variaveis'] = var_list
                    new_objetcs['identificador'] = objeto['identificador']
                    all_objects.append(new_objetcs)

            if tem_var == 1:
                tipo_obj.append({
                    'tipoObjeto': str(tipo["tipoObjeto"]),
                    'objetos': all_objects
                })

        req_body[0]['tipoObjetos'] = tipo_obj

        return req_body

    def request_variables(self) -> list[dict]:
        req = requests.get(self.url_req_variables)
        res = req.json()

        req_obj = []

        for index, obj in enumerate(res):
            req_obj.append({
                "tipoObjeto": res[index]['tipo'],
                "objetos": res[index]['objetos']
            })
        return req_obj
