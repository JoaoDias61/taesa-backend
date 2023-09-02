import requests
import json


class ECM:
    url_homolog = "https://b31d-177-188-106-192.ngrok-free.app/"
    url_prod = "http://172.21.184.6:8088/"
    url_base = url_homolog
    url_req_variables = url_base + "api/eam/obter/variaveis"
    url_req_results = url_base + "api/eam/obter/dadosvariaveis"

    def request_results(self, start_dt: str, end_dt: str, ecm_id: int) -> list:
        json_req = self.build_request(start_dt, end_dt, ecm_id)
        headers = {
            'Content-Type': 'application/json'
        }

        res = requests.post(
            url=ECM.url_req_results,
            data=json.dumps(json_req),
            headers=headers
        )

        print("response" + str(res))

        return self.filter_nan(res.json())

    @staticmethod
    def filter_nan(response: list[dict]) -> list[dict]:
        for tipo in response[0]["tipoObjetos"]:
            for obj in tipo['objetos']:
                for k in obj['variaveis']:
                    if 'valor' in k:
                        if k['valor'] == 'NaN' or k['valor'] == '':
                            k.pop('valor')
                    else: 
                        pass

        return response



    def build_request(self, start_dt: str, end_dt: str, ecm_id: int) -> list[dict]:
        ecm_variables = self.request_variables()

        req_body = [
            {
                "equipamentoSigmaId": ecm_id,
                "tipoObjetos": ecm_variables
            }
        ]

        for tipo in req_body[0]["tipoObjetos"]:
            for obj in tipo['objetos']:
                if 'codigo' in obj:
                    obj.pop('codigo')

                if 'descricao' in obj:
                    obj.pop('descricao')

                for k in obj['variaveis']:
                    k['dataInicio'] = start_dt
                    k['dataFim'] = end_dt
                    k['tipoRetorno'] = "1"

                    if 'unidadeMedida' in k:
                        k.pop('unidadeMedida')
                    if 'descricao' in k:
                        k.pop('descricao')

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
