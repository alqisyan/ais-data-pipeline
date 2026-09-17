from datetime import datetime
import json
from kafka import KafkaConsumer, JsonSerializer
import psycopg

consumer = KafkaConsumer(
    'ais-raw',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ais-data',
    fetch_min_bytes=1,
    fetch_max_wait_ms=100,
    max_poll_records=10, 
    value_deserializer=JsonSerializer()
)

def data_mapping(message):
    try:
        payload = message
        messageID = payload.get("mainCalculator", {}).get("messageID", {})

        match messageID:
            case "Static Data Report":
                print("Static Data Report:", message)
            case "Static Voyage Data":
                print("Static Voyage Data:", message)
            case "Extended Class B position report":
                print("Extended Class B position report:", message)
            case _:
                print("Message Type 1/2/3:", message)
        return messageID, message
    except json.JSONDecodeError:
        print("Invalid JSON:", message)
        return None, None

def connectDB():
    print("Connecting to Database")
    try:
        conn = psycopg.connect(
            host = "postgres",
            port = 5432,
            dbname = "mydatabase",
            user = "myuser",
            password = "mysecretpassword"
        )
        return conn
    except psycopg.Error as e:
        print("Database connection failed : {e}")
        raise

def create_Table(conn):
    print("Creating table if not exist")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS ais;
            CREATE TABLE IF NOT EXISTS ais.data(
                mmsi Integer PRIMARY KEY,
                messageID text,
                flag      text,
                repeat    text,
                navStatus text,
                shipType  text,
                rot       Integer,
                sog       Integer,
                accuracy  Integer,
                lon       Float,
                lat       Float,
                cog       Integer,
                dimension Integer,
                truHead   Integer,
                imo       Integer,
                callSign  text,
                name      text,
                draugh    Integer,
                destination text,
                aisClass  text,
                createdAt TIMESTAMP DEFAULT NOW(),
                updatedAt TIMESTAMP
            );

            """
        )
        conn.commit()
        print("Table is created")
    except psycopg.Error as e:
        print("Database connection failed : {e}")
        raise

def input_data(cursor, message):
    nested_data = message.get("mainCalculator", {})
    mmsi = nested_data.get("mmsi")
    messageID = nested_data.get("messageID")
    flag = message.get("country")
    repeat = nested_data.get("repeat")
    navStatus = nested_data.get("navStatus")
    shipType = nested_data.get("shipType")
    rot = nested_data.get("rot")
    sog = nested_data.get("sog")
    accuracy = nested_data.get("accuracy")
    lon = nested_data.get("lon")
    lat = nested_data.get("lat")
    cog = nested_data.get("cog")
    dimension = nested_data.get("dimension")
    truHead = nested_data.get("trueHead")
    imo = nested_data.get("imo")
    callSign = nested_data.get("callsign")
    name = nested_data.get("name")
    draugh = nested_data.get("draugh")
    destination = nested_data.get("destination")
    aisClass = message.get("classType")
    categories = message.get("categories")

    now = datetime.now()
    print("Inputing Data")
    print(f"""Categories: {categories}, AIS Class: {aisClass}, MMSI: {mmsi}, CallSign: {callSign}, IMO: {imo}, Repeat: {repeat},
        MessageID: {messageID}, Flag: {flag}, NavStatus: {navStatus}, Lon: {lon}, Lat: {lat}, ROT: {rot}, Draft: {draugh}, Ship Type: {shipType},
        SOG: {sog}, COG: {cog}, truHead: {truHead}, accuracy: {accuracy}, Dimension: {dimension}, Name: {name}, Destination: {destination}
        """)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ais.data (
                mmsi,
                messageID,
                flag,
                repeat,
                navStatus,
                shipType,
                rot,
                sog,
                accuracy,
                lon,
                lat,
                cog,
                dimension,
                truHead,
                imo,
                callSign,
                name,
                draugh,
                destination,
                aisClass,
                createdAt,
                updatedAt
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (mmsi)
            DO UPDATE SET lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                sog = EXCLUDED.sog,
                cog = EXCLUDED.cog,
                rot = EXCLUDED.rot,
                name = EXCLUDED.name,
                navStatus = EXCLUDED.navStatus,
                truHead = EXCLUDED.truHead,
                destination = EXCLUDED.destination;
            """, (mmsi,
                  messageID,
                            flag,
                            repeat,
                            navStatus,
                            shipType,
                            rot,
                            sog,
                            accuracy,
                            lon,
                            lat,
                            cog,
                            dimension,
                            truHead,
                            imo,
                            callSign,
                            name,
                            draugh,
                            destination,
                            aisClass,
                            now,
                            now,
                  )
        )
    except psycopg.Error as e:
        print("Error Insert Data into Database : {e}")
        raise

conn = connectDB()
#create_Table(conn)

for msg in consumer:
    message_id, message = data_mapping(msg.value)
    if message_id and message:
        input_data(conn, message)
        conn.commit()