from pydantic import BaseModel

class TransactionFraud(BaseModel):
    id_transaction: int
    client: str
    is_night: bool
    first_pattern: bool
    second_pattern: bool
    third_pattern: bool

class Params(BaseModel):
    list_id_transaction: list[int] = None
    count_time_difference_max: int
    time_difference_seconds: int
    time_difference_minutes: int
    threshold_amount: float