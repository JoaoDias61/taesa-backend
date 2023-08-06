from pydantic import BaseModel
from typing import Optional

class HealthIndexPerEquipment(BaseModel):
    family: str

class HealthIndexPerSubsystem(BaseModel):
    family: str
    description: str
    subsystem: str
    initial_date: str
    final_date: str

class HealthIndexPerSubsystem(BaseModel):
    family: str
    id_equipment: int
    initial_date: str
    final_date: str
    subsystem: Optional[str] = None

class HealthIndexAllSubsystem(BaseModel):
    id_equipment: int
    
class CalculateAgeingWaterOilFormation(BaseModel):
    id_equipment: int

class RiskMatrix(BaseModel):
    family: Optional[str] = None, 
    id_equipment: Optional[int] = None

class RiskMatrixHistoric(BaseModel):
    id_equipment: int

class EvolutionTimeList(BaseModel):
    id_equipment: int
    initial_date: str
    final_date: str