from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from database import create_tables, delete_tables
from transactions.router import router as transactions_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_tables()
    print("База готова")
    yield
    await delete_tables()
    print("База очищена")

app = FastAPI(title="Transaction", lifespan=lifespan)

app.include_router(transactions_router)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)