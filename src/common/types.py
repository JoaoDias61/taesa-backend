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
    description: str
    initial_date: str
    final_date: str
    subsystem: Optional[str] = None
    
class CalculateAgeingWaterOilFormation(BaseModel):
    description: str

class RiskMatrix(BaseModel):
    family: Optional[str] = None, 
    description: Optional[str] = None

class RiskMatrixHistoric(BaseModel):
    description: str
