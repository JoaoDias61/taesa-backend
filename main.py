from src.model.calculate_transformer_information import calculate_transformer_information
from src.model.health_index_per_equipment import health_index_per_equipment 
from src.model.health_index_per_subsystem import health_index_per_subsystem

from src.model.risk_matrix import risk_matrix
from src.model.filter_data import filter_data

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
    familia: str
    descricao: str

@app.get("/filterData")
def get_data():
    data_processor = filter_data(cursor)
    data = data_processor.filter_data_exec()
    grouped_data = data.groupby('nomeFamilia')['Descricao'].apply(list).reset_index()
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

@app.post("/risk_matrix/")
async def calculate_risk_matrix(data: CalculateInput):
          descricao = data.descricao
          familia = data.familia
          resultado = risk_matrix(cursor, descricao, familia).risk_matrix_exec()
          return resultado.to_dict(orient='list')


@app.post("/calculate_transformer_information/")
async def calculate_transformer_information_post(data: CalculateInput):
          descricao = data.descricao
          familia = data.familia
          resultado = calculate_transformer_information(cursor, descricao, familia).calculate_transformer_information_exec()
          return resultado.to_dict()

@app.post("/health_index_per_equipment/")
async def health_index_per_equipment_post(data: CalculateInput):
          descricao = data.descricao
          familia = data.familia
          calculate_health = health_index_per_equipment(cursor, descricao, familia).health_index_per_equipment_exec()
          resultado = calculate_health
          return resultado.to_dict(orient='list')

@app.post("/health_index_per_subsystem/")
async def health_index_per_subsystem_post(data: CalculateInput):
          descricao = data.descricao
          familia = data.familia
          calculate_health = health_index_per_subsystem(cursor, descricao, familia).health_index_per_subsystem_exec()
          resultado = calculate_health
          return resultado.to_dict(orient='list')