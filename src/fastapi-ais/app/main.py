from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from aiokafka import AIOKafkaConsumer
import json
import asyncio

app = FastAPI()
latest_data = {}

async def consume_kafka():
    consumer = AIOKafkaConsumer(
        "ais-raw",
        bootstrap_servers="kafka:29092",
        group_id="ais-data",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = msg.value
            ship_id = str(payload.get("mainCalculator", {}).get("mmsi")).strip()
            latest_data[ship_id] = payload
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_kafka())

@app.get("/")
def welcome():
    return {"message": "Welcome to AIS FastAPI"}

@app.get("/ships/{ship_id}")
def get_ship(ship_id: str):
    return ship_id, latest_data.get(ship_id, "Not Found")

@app.get("/ships")
def get_all_ships():
    return latest_data

@app.get("/ships_ndjson")
def get_ships_ndjson():
    def generate():
        for ship_id, ship in latest_data.items():
            yield json.dumps(ship) + "\n"
    return StreamingResponse(generate(), media_type="application/x-ndjson")
