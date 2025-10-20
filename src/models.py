from pydantic import BaseModel, Field, RootModel
from typing import Any, Dict, Optional, List
from datetime import datetime


class Event(BaseModel):
    topic: str = Field(...)
    event_id: str = Field(...)
    timestamp: datetime = Field(...)
    source: Optional[str]
    payload: Dict[str, Any]


# Gunakan RootModel di Pydantic v2
class PublishBatch(RootModel[List[Event]]):
    pass
