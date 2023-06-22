from src.model.filterData import DataProcessor

from dotenv import load_dotenv
from fastapi import FastAPI
import uvicorn
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

@app.get("/api/data")
def get_data():
    data_processor = DataProcessor(cursor)
    data = data_processor.process_data()
    grouped_data = data.groupby('nomeFamilia')['codigoOperacional'].apply(list).reset_index()
    print(grouped_data)
    return grouped_data.to_dict(orient='records')