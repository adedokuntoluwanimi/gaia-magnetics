from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.jobs import router as jobs_router


app = FastAPI(
    title="GAIA Magnetics Backend",
    version="1.0.0",
)

# CORS (frontend hosted separately)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(
    jobs_router,
    prefix="/jobs",
    tags=["jobs"],
)


@app.get("/health")
def health_check():
    return {"status": "ok"}
