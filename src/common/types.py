from pydantic import BaseModel
from typing import Optional

class HealthIndexPerEquipment(BaseModel):
    id_equipment: int

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
    family: Optional[str] = None,  # type: ignore
    id_equipment: Optional[int] = None

class RiskMatrixHistoric(BaseModel):
    id_equipment: int

class EvolutionTimeList(BaseModel):
    id_equipment: int
    identifier: int
    initial_date: str
    final_date: str

class LatestMaintenance(BaseModel):
    id_equipment: int

class ProbabilityFailureStatus(BaseModel):
    id_equipment: int

class CalculateOptimalDate(BaseModel):
    id_equipment: int
    subsystem: str
    cost_PM: float
    cost_CM: float