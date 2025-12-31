from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.jobs import router as jobs_router


app = FastAPI(
    title="GAIA Magnetics Backend",
    version="1.0.0",
)


# -------------------------------------------------
# CORS (required for frontend integration)
# -------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten later in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
