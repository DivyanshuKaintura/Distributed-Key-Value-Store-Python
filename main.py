"""
Very basic key-value store to test AWS deployment (single instance, no Raft yet).
Endpoints:
  GET    /kv/{key}   -> read a value
  POST   /kv/{key}   -> write a value  (body: {"value": "..."})
  DELETE /kv/{key}   -> delete a value
  GET    /health      -> basic health check
"""


from in_memory_store import in_memory_store
from fastapi import FastAPI
from utils import logger

app = FastAPI()
store = in_memory_store()

@app.get("/")
def root():
    logger.info("Get request received")
    return {"message": "Hello World"}
  
  
@app.get("/health")
def health():
    logger.info("Health check request received")
    return {"status": "healthy"}

  
@app.get("/kv/{key}")
def get(key: str):
    logger.info(f"Get request received for key: {key}")
    if key in store._store:
      return {"key": key, "value": store.get(key)}
    else:
      logger.error(f"Key not found: {key}")
      return {"error": "Key not found"}, 404
    
@app.post("/kv/{key}")
def post(key: str, value: str):
  logger.info(f"Post request received for key: {key} with value: {value}")
  store.put(key, value)
  return {"message": "Value set successfully"}

@app.delete("/kv/{key}")
def delete(key: str):
  logger.info(f"Delete request received for key: {key}")
  if key in store._store:
    store.delete(key)
    return {"message": "Value deleted successfully"}
  else:
    logger.error(f"Key not found: {key}")
    return {"error": "Key not found"}, 404