# Auto-generated ASGI entrypoint for Railway & Nixpacks
import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from api.index import app as api_app

app = api_app

@app.get("/", response_class=HTMLResponse)
def serve_root_frontend():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>Project Backend Online</h1>"
