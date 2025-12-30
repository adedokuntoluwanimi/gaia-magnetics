# app/main.py

from fastapi import FastAPI
from app.api.jobs import router as jobs_router


app = FastAPI(
    title="GAIA Magnetics Backend",
    version="1.0.0",
)


# -------------------------------------------------
# Routers
# -------------------------------------------------

app.include_router(jobs_router)


# -------------------------------------------------
# Health check
# -------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}
