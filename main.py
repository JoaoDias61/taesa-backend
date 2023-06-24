from src.model.calculateTransformerInformation import calculateTransformerInformation
from src.model.riskMatrix import riskMatrix
from src.model.filterData import filterData

from dotenv import load_dotenv
from pydantic import BaseModel
from fastapi import FastAPI
import pyodbc
import os

load_dotenv('.env')

server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
driver = os.getenv('DRIVER')

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=Yes"
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

app = FastAPI()

class CalculateInput(BaseModel):
    codigo_operacional: str
    familia: str

@app.get("/filterData")
def get_data():
    data_processor = filterData(cursor)
    data = data_processor.process_data()
    grouped_data = data.groupby('nomeFamilia')['codigoOperacional'].apply(list).reset_index()
    result = grouped_data.to_dict(orient='records')
    info_adicional = {
        "subsistemas": [
            "Parte Ativa",
            "Comutador sobrecarga",
            "Parte Ativa",
            "Acessórios",
            "Ambiente",
            "Tanque",
            "Resfriamento",
            "Preservação do Óleo Isolante",
            "Bucha"
        ]
    }
    result.append(info_adicional)
    return result

@app.post("/riskMatrix/")
async def calculate_transformer_information(data: CalculateInput):
          codigo_operacional = data.codigo_operacional
          familia = data.familia
          resultado = riskMatrix(cursor, codigo_operacional, familia).processRiskMatrix()
          return resultado.to_dict(orient='list')


@app.post("/calculateTransformerInformation/")
async def calculate_transformer_information(data: CalculateInput):
          codigo_operacional = data.codigo_operacional
          familia = data.familia
          resultado = calculateTransformerInformation(cursor, codigo_operacional, familia).executar_calculo()
          return resultado.to_dict()