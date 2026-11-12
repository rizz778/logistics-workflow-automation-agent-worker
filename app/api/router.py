from fastapi import APIRouter
from api.routes.upload import router as upload_router
from api.routes.status import router as status_router

api_router = APIRouter()
api_router.include_router(upload_router)
api_router.include_router(status_router)
