from src.model.calculate_ageing_water_oil_formation import calculate_ageing_water_oil_formation
from src.model.calculate_transformer_information import calculate_transformer_information
from src.model.health_index_per_equipment import health_index_per_equipment 
from src.model.health_index_per_subsystem import health_index_per_subsystem

from src.model.risk_matrix import risk_matrix
from src.model.filter_data import filter_data

from dotenv import load_dotenv
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

@app.get("/filter_data")
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

@app.post("/calculate_ageing_water_oil_formation/")
async def calculate_ageing_water_oil_formation_post(descricao: str):
          calculate_health = calculate_ageing_water_oil_formation(cursor, descricao).calculate_ageing_water_oil_formation_exec()
          return calculate_health.to_dict(orient='list')


@app.post("/health_index_per_equipment/")
async def health_index_per_equipment_post(familia: str, descricao: str):
          calculate_health = health_index_per_equipment(cursor, descricao, familia).health_index_per_equipment_exec()
          resultado = calculate_health
          return resultado.to_dict(orient='list')

@app.post("/health_index_per_subsystem/")
async def health_index_per_subsystem_post(familia: str, descricao: str):
          calculate_health = health_index_per_subsystem(cursor, descricao, familia).health_index_per_subsystem_exec()
          resultado = calculate_health
          return resultado.to_dict(orient='list')

@app.post("/risk_matrix/")
async def calculate_risk_matrix(familia: str, descricao: str):
          resultado = risk_matrix(cursor, descricao, familia).risk_matrix_exec()
          return resultado.to_dict(orient='list')