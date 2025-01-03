from fastapi import FastAPI

# Initialize the FastAPI app
app = FastAPI()

# Import your routes or any other logic here
from app.api.endpoints import router

# Include the router for the API
app.include_router(router)
