from fastapi import FastAPI

app = FastAPI(title="KG Medallion API")

@app.get("/health")
def health():
    return {"status": "ok"}
