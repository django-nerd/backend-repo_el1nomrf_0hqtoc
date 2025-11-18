import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

app = FastAPI(title="Data Infra SaaS Orchestrator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Service(BaseModel):
    id: str
    name: str
    summary: str
    homepage: str
    docs: str
    status: str = "available"


# Static catalog for this demo environment
SERVICES: List[Service] = [
    Service(
        id="flink",
        name="Apache Flink",
        summary="Stateful computations over data streams with low latency and high throughput.",
        homepage="https://flink.apache.org/",
        docs="https://nightlies.apache.org/flink/",
    ),
    Service(
        id="amoro",
        name="Apache Amoro",
        summary="Lakehouse table management for Apache Iceberg and mixed engines.",
        homepage="https://amoro.apache.org/",
        docs="https://amoro.apache.org/docs/",
    ),
    Service(
        id="starrocks",
        name="StarRocks",
        summary="High-performance MPP analytics database for real-time OLAP.",
        homepage="https://www.starrocks.io/",
        docs="https://docs.starrocks.io/",
    ),
    Service(
        id="superset",
        name="Apache Superset",
        summary="Modern data exploration and visualization platform.",
        homepage="https://superset.apache.org/",
        docs="https://superset.apache.org/docs/intro",
    ),
]


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI Backend!"}


@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}


@app.get("/api/services", response_model=List[Service])
def list_services():
    """Return the catalog of available backend-managed services."""
    return SERVICES


@app.get("/api/services/{service_id}", response_model=Service)
def get_service(service_id: str):
    for svc in SERVICES:
        if svc.id == service_id:
            return svc
    raise HTTPException(status_code=404, detail="Service not found")


@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }
    
    try:
        # Try to import database module
        from database import db
        
        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"
            
            # Try to list collections to verify connectivity
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]  # Show first 10 collections
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"
            
    except ImportError:
        response["database"] = "❌ Database module not found (run enable-database first)"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"
    
    # Check environment variables
    import os
    response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"
    
    return response


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
