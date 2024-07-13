from pydantic import BaseModel

class TransactionFraud(BaseModel):
    id_transaction: int
    client: str
    first_pattern: bool
    second_pattern: bool
    third_pattern: bool