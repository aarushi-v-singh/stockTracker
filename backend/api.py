from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# ADD THIS PART:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows your React app to talk to the API
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/prices") # or /data, whichever your frontend hits
def get_data():
    try:
        # 1. Manually provide names so the JSON keys are consistent
        df = pd.read_csv("/data/data.csv", names=["timestamp", "price"])
        
        # 2. Convert to a list of objects: [{"timestamp": "...", "price": ...}, ...]
        data = df.tail(20).to_dict(orient="records")
        
        return data
    except Exception as e:
        print(f"Error: {e}")
        return []