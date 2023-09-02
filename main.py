from src.model.calculate_optimal_date import calculate_optimal_date
from src.model.probability_failure_status import probability_failure_status
from src.model.scheduled_maintenance import scheduled_maintenance
from src.model.latest_maintenance import latest_maintenance
from src.model.evolution_time_list import evolution_time_list
from src.model.evolution_time import evolution_time
from src.model.health_index_all_subsystem import health_index_all_subsystem
from src.model.risk_matrix_historic import risk_matrix_historic
from src.common.types import CalculateAgeingWaterOilFormation, CalculateOptimalDate, EvolutionTimeList, HealthIndexAllSubsystem, HealthIndexPerEquipment, HealthIndexPerSubsystem, LatestMaintenance, RiskMatrix, RiskMatrixHistoric
from src.model.calculate_ageing_water_oil_formation import calculate_ageing_water_oil_formation
from src.model.health_index_per_equipment import health_index_per_equipment
from src.model.health_index_per_subsystem import health_index_per_subsystem
from fastapi.middleware.cors import CORSMiddleware

from src.model.risk_matrix import risk_matrix
from src.model.filter_data import filter_data
from fastapi import FastAPI

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
            "subsystems":INFO_SUB
        }

        return result_dict

    output = format_data(data)
    return output


@app.post("/health_index_per_equipment/")
async def health_index_per_equipment_post(request: HealthIndexPerEquipment):
    calculate_health = health_index_per_equipment(
        cursor, request.id_equipment).health_index_per_equipment_exec()
    result = calculate_health
    return result.to_dict(orient='records')


@app.post("/health_index_per_subsystem/")
async def health_index_per_subsystem_post(request: HealthIndexPerSubsystem):
    calculate_health = health_index_per_subsystem(
        cursor, request.initial_date, request.final_date, request.family, request.id_equipment, request.subsystem).health_index_per_subsystem_exec()
    return calculate_health.to_dict(orient='records')

@app.post("/health_index_all_subsystem/")
async def health_index_all_subsystem_post(request: HealthIndexAllSubsystem):
    calculate_health = health_index_all_subsystem(
        cursor, request.id_equipment).health_index_all_subsystem_exec()
    calculate_health_result = calculate_health.to_dict(orient='records')
    return calculate_health_result

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
    result = risk_matrix_historic(cursor, request.id_equipment)
    filter_result = result.risk_matrix_historic_exec()
    return filter_result

@app.post("/evolution_time")
async def request_evolution_time(request: RiskMatrixHistoric):
    result = evolution_time(cursor, request.id_equipment)
    filter_result = result.evolution_time_exec()
    return filter_result

@app.post("/evolution_time_list")
async def request_evolution_time_list(request: EvolutionTimeList):
    result = evolution_time_list(cursor, request.id_equipment, request.identifier, request.initial_date, request.final_date)
    filter_result = result.evolution_time_list_exec()
    return filter_result

@app.post("/latest_maintenance")
async def latest_maintenance_list(request: LatestMaintenance):
    result = latest_maintenance(cursor, request.id_equipment)
    filter_result = result.latest_maintenance_exec()
    return filter_result.to_dict(orient='records')

@app.post("/scheduled_maintenance")
async def scheduled_maintenance_list(request: LatestMaintenance):
    result = scheduled_maintenance(cursor, request.id_equipment)
    filter_result = result.scheduled_maintenance_exec()
    return filter_result.to_dict(orient='records')

@app.post("/probability_failure_status")
async def probability_failure_status_list(request: LatestMaintenance):
    result = probability_failure_status(cursor, request.id_equipment)
    filter_result = result.probability_failure_status_exec()
    return filter_result

@app.post("/calculate_optimal_date")
async def calculate_optimal_date_list(request: CalculateOptimalDate):
    result = calculate_optimal_date(cursor, request.id_equipment, request.subsystem, request.cost_PM, request.cost_CM)
    filter_result = result.calculate_optimal_date_exec()
    return filter_result




