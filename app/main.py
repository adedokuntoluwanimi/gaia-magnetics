from fastapi import FastAPI
from app.api.jobs import router as jobs_router

app = FastAPI(title="GAIA Magnetics Backend")

# -------------------------
# Health check
# -------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -------------------------
# API routes
# -------------------------
app.include_router(
    jobs_router,
    prefix="/jobs",
    tags=["jobs"],
)
