from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import asyncio
import uuid
import tempfile
import os
import shutil
import subprocess
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"), # Guarda en archivo
        logging.StreamHandler()         # Muestra en consola
    ]
)
logger = logging.getLogger(__name__)

# 1. El estado en RAM (Base de Datos temporal)
# Guardará: {"id_tarea": {"state": "pending", "result": "/ruta/xml", "error": None}}
memory_data = {}

# 2. La Cola en RAM (temporal)
queue_tasks = asyncio.Queue()

async def procesar_cola():
    while True:
        task_id, file_path, temp_dir = await queue_tasks.get()
        memory_data[task_id]["state"] = "processing"
        try:
            #this runs two scripts, one anfter each other, and waits for them to finish before continuing
            script_deteccion   = os.path.abspath("../bfaaap/detectionintegration.py")
            script_integracion = os.path.abspath("../bfaaap/systemintegration.py")
            await asyncio.to_thread(
                subprocess.run, 
                ["python3", script_deteccion, file_path], 
                capture_output=True, check=True, text=True
            )
            await asyncio.to_thread(
                subprocess.run, 
                ["python3", script_integracion, file_path], 
                capture_output=True, check=True, text=True
            )
            name     = os.path.splitext(os.path.basename(file_path))[0]
            xml_path = os.path.join(temp_dir, "xml", f"{name}_staves1and2_combined.xml")
            if os.path.exists(xml_path):
                memory_data[task_id]["state"] = "completed"
                memory_data[task_id]["result"] = xml_path
            else:
                raise FileNotFoundError("Script terminó pero no generó el XML.")
        except Exception as e:
            memory_data[task_id]["state"] = "error"
            memory_data[task_id]["error"] = str(e)      
        finally:
            queue_tasks.task_done()
            
@asynccontextmanager
async def lifespan(app: FastAPI):
    tarea_worker = asyncio.create_task(procesar_cola())
    yield # Para que FastAPI se quede recibiendo peticiones web...
    tarea_worker.cancel()

app = FastAPI(title="OMR Web Service - In Memory Queue", lifespan=lifespan)

@app.post("/api/v1/scores", status_code=202)
async def omr_api(file: UploadFile = File(...)):
    task_id   = str(uuid.uuid4())
    temp_dir  = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    # Registrar en la "BD" simulada (después se deberá usar algo como redis)
    memory_data[task_id] = {
        "state": "pending",
        "result": None,
        "error": None
    }
    await queue_tasks.put((task_id, file_path, temp_dir))
    logger.info(f"Task queued: {task_id}")
    return {"task_id": task_id, "state": "pending", "message": "Task queued successfully"}

@app.get("/")
async def index():
    return "hola"

@app.get("/api/v1/scores/{task_id}")
async def consultar_tarea(task_id: str):
    if task_id not in memory_data:
        raise HTTPException(status_code=404, detail="Task not found")
    tarea = memory_data[task_id]
    if tarea["state"] == "completed":
        return FileResponse(
            path=tarea["result"], 
            media_type="application/vnd.recordare.musicxml+xml",
            filename=f"partitura_{task_id}.xml"
        )
    elif tarea["state"] == "error":
        raise HTTPException(status_code=500, detail=tarea["error"])
    else:
        return {"task_id": task_id, "state": tarea["state"], "message": "Task is still being processed"}