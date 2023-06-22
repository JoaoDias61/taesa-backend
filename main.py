from src.model.calculateTransformerInformation import calculateTransformerInformation
from src.model.filterData import filterData

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

@app.get("/filterData")
def get_data():
    data_processor = filterData(cursor)
    data = data_processor.process_data()
    grouped_data = data.groupby('nomeFamilia')['codigoOperacional'].apply(list).reset_index()
    return grouped_data.to_dict(orient='records')

@app.post("/calculateTransformerInformation/")
async def calculate_transformer_information(codigo_operacional: str, familia: str):
          resultado = calculateTransformerInformation(cursor, codigo_operacional, familia).executar_calculo()
          return resultado.to_dict()