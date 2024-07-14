from fastapi import APIRouter, UploadFile, File

from transactions.schemas import Params
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
async def run_find_fraud(params: Params):
    result = await TransactionRepository.run_find_fraud(params)
    return result

@router.post("/get_client_by_id")
async def get_client_by_id(client_id: str):
    result = await TransactionRepository.get_client_by_id(client_id)
    return result