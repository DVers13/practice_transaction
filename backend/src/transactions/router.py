from fastapi import APIRouter, UploadFile, File

from transactions.repository import TransactionRepository


router = APIRouter(
    prefix="/transaction",
    tags=["Транзакции"],
)


@router.post("/upload_csv/")
async def add_task(file: UploadFile = File(...)):
    result = await TransactionRepository.upload_csv(file)
    return result

@router.post("/run_find_fraud")
async def run_find_fraud(list_id_transaction: list[int] = None):
    result = await TransactionRepository.run_find_fraud(list_id_transaction)
    return result