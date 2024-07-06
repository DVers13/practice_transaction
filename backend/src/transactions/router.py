from fastapi import APIRouter, Depends, UploadFile, File

from transactions.repository import TransactionRepository


router = APIRouter(
    prefix="/transaction",
    tags=["Транзакции"],
)


@router.post("/upload_csv/")
async def add_task(file: UploadFile = File(...)):
    result = await TransactionRepository.upload_csv(file)
    return result

@router.get("/run_process_transactions")
async def run_process_transactions():
    result = await TransactionRepository.run_process_transactions()
    return result