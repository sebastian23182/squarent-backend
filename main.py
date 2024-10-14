import os
from fastapi import FastAPI
from routers import finca_raiz
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv('ALLOWED_ORIGIN')],
    allow_credentials=True,
    allow_methods=['POST'],
    allow_headers=['*'],
)

app.include_router(finca_raiz.router)