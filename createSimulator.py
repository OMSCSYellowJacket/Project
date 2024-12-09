import json
from fastapi import FastAPI
from fastapi.responses import StreamingResponse 
import time

file_path ='dataupdates2024-09-13v2.txt'
def import_txt_as_json(file_path):
    """Imports a .txt file containing multiple JSON objects as a list of Python dictionaries."""

    with open(file_path, 'r') as file:
        data_dict = dict() 
        data = []
        for line in file:
            try:
                json_obj = json.loads(line)
                
                if 'data' in list(json_obj.keys()):
                    print("LINE 1:", json_obj['data'])
                    data_dict[json_obj['data'][0]['timestamp']] = json_obj
                data.append(json_obj)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON: {line}")

    return data_dict

app = FastAPI()
def generate_data():
    json_data = import_txt_as_json(file_path)
    for key in json_data.keys():
        #if streaming_active:
        json.dumps(json_data[key]['data'], indent = 4)
        yield f"{json.dumps(json_data[key]['data'], indent = 4)}\n\n"
        time.sleep(1)

streaming_active = True  # Global flag to control streaming
@app.get("/stream")
def stream():
    return StreamingResponse(generate_data(), media_type="text/event-stream")

