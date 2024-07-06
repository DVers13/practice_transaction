from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class TransactionAnalysis(BaseModel):
    client: str
    failures: int
    time_diff: datetime
    time_diff_count: int
    amount_diff_count: int