from src.common.types import CalculateAgeingWaterOilFormation, HealthIndexPerEquipment, HealthIndexPerSubsystem, RiskMatrix
from src.model.calculate_ageing_water_oil_formation import calculate_ageing_water_oil_formation
from src.model.health_index_per_equipment import health_index_per_equipment
from src.model.health_index_per_subsystem import health_index_per_subsystem
from fastapi.middleware.cors import CORSMiddleware

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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


INFO_SUB = [
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

@app.get("/filter_data")
def get_data():
    data_processor = filter_data(cursor)
    data = data_processor.filter_data_exec()
    grouped_data = data.groupby('familyName')[
        'description'].apply(list).reset_index()
    result = grouped_data.to_dict(orient='records')
    json_info = {
        "familyEquipment": result,
        "subsystems": INFO_SUB
    }
    return json_info


@app.post("/health_index_per_equipment/")
async def health_index_per_equipment_post(request: HealthIndexPerEquipment):
    calculate_health = health_index_per_equipment(
        cursor, request.family).health_index_per_equipment_exec()
    result = calculate_health
    return result.to_dict(orient='records')


@app.post("/health_index_per_subsystem/")
async def health_index_per_subsystem_post(request: HealthIndexPerSubsystem):
    calculate_health = health_index_per_subsystem(
        cursor, request.initial_date, request.final_date, request.family, request.description).health_index_per_subsystem_exec()
    return calculate_health.to_dict(orient='records')


@app.post("/calculate_ageing_water_oil_formation/")
async def calculate_ageing_water_oil_formation_post(request: CalculateAgeingWaterOilFormation):
    calculate_health = calculate_ageing_water_oil_formation(
        cursor, request.description).calculate_ageing_water_oil_formation_exec()
    return calculate_health.to_dict(orient='records')


@app.post("/risk_matrix")
async def calculate_risk_matrix(request: RiskMatrix):
    result = risk_matrix(cursor, request.description, request.family)
    filter_result = result.risk_matrix_exec()
    return filter_result.to_dict(orient='records')