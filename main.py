from src.model.risk_matrix_historic import risk_matrix_historic
from src.common.types import CalculateAgeingWaterOilFormation, HealthIndexPerEquipment, HealthIndexPerSubsystem, RiskMatrix, RiskMatrixHistoric
from src.model.calculate_ageing_water_oil_formation import calculate_ageing_water_oil_formation
from src.model.health_index_per_equipment import health_index_per_equipment
from src.model.health_index_per_subsystem import health_index_per_subsystem
from fastapi.middleware.cors import CORSMiddleware

from src.model.risk_matrix import risk_matrix
from src.model.filter_data import filter_data
import re
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from dotenv import load_dotenv
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

    def format_data(data):

        def check_category(descricao, category):
            return category in descricao

        grouped_data = data.groupby('Nome').apply(lambda group: {
            'Nome': group['Nome'].iloc[0],
            'Descricao': [{
                'descricao': row['Descricao'],
                'id': row['Id']
            } for _, row in group.iterrows() if check_category(row['Descricao'], group['Nome'].iloc[0])]
        }).reset_index(name='familyEquipment')

        result_dict = {
            "familyEquipment": grouped_data['familyEquipment'].tolist(),
            "subsystems": [
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

        return result_dict

    output = format_data(data)
    return output


@app.post("/health_index_per_equipment/")
async def health_index_per_equipment_post(request: HealthIndexPerEquipment):
    calculate_health = health_index_per_equipment(
        cursor, request.family).health_index_per_equipment_exec()
    result = calculate_health
    return result.to_dict(orient='records')


@app.post("/health_index_per_subsystem/")
async def health_index_per_subsystem_post(request: HealthIndexPerSubsystem):
    calculate_health = health_index_per_subsystem(
        cursor, request.initial_date, request.final_date, request.family, request.id_equipment, request.subsystem).health_index_per_subsystem_exec()
    return calculate_health.to_dict(orient='records')

@app.post("/calculate_ageing_water_oil_formation/")
async def calculate_ageing_water_oil_formation_post(request: CalculateAgeingWaterOilFormation):
    calculate_health = calculate_ageing_water_oil_formation(
        cursor, request.id_equipment).calculate_ageing_water_oil_formation_exec()
    return calculate_health.to_dict(orient='records')

@app.post("/risk_matrix")
async def calculate_risk_matrix(request: RiskMatrix):
    result = risk_matrix(cursor, request.id_equipment, request.family)
    filter_result = result.risk_matrix_exec()
    return filter_result.to_dict(orient='records')

@app.post("/risk_matrix_historic")
async def calculate_risk_matrix_historic(request: RiskMatrixHistoric):
    result = risk_matrix_historic(cursor, request.description)
    filter_result = result.risk_matrix_historic_exec()
    return filter_result.to_dict(orient='records')