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

def defaultAIS(conn, data):
    cursor = conn.cursor()
    query = """
        INSERT INTO ais.data (mmsi, messageID, repeat, flag, navStatus, rot, sog, cog, lon, lat, accuracy, truHead, aisClass)
        VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (mmsi) 
        DO UPDATE SET 
            navStatus = COALESCE(EXCLUDED.navStatus, ais.data.navStatus),
            rot = COALESCE(EXCLUDED.rot, ais.data.rot),
            sog = COALESCE(EXCLUDED.sog, ais.data.sog),
            cog = COALESCE(EXCLUDED.cog, ais.data.cog),
            lon = COALESCE(EXCLUDED.lon, ais.data.lon),
            lat = COALESCE(EXCLUDED.lat, ais.data.lat),
            truHead = COALESCE(EXCLUDED.truHead, ais.data.truHead),
            accuracy = COALESCE(EXCLUDED.accuracy, ais.data.accuracy),
            aisClass = EXCLUDED.aisClass;
    """
    values = (
        data.get("mainCalculator").get("mmsi"),
        data.get("mainCalculator").get("messageID"),
        data.get("mainCalculator").get("repeat"),
        data.get("country"),
        data.get("mainCalculator").get("navStatus"),
        data.get("mainCalculator").get("rot"),
        data.get("mainCalculator").get("sog"),
        data.get("mainCalculator").get("cog"),
        data.get("mainCalculator").get("lon"),
        data.get("mainCalculator").get("lat"),
        data.get("mainCalculator").get("accuracy"),
        data.get("mainCalculator").get("truHead"),
        data.get("classType")
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    print(f"[DB Success] Inserted Data with MMSI: {data.get("mainCalculator").get("mmsi")}")

def StaticVoyageData(conn, data):
    cursor = conn.cursor()
    query = """
        INSERT INTO ais.data (mmsi, messageID, repeat, flag, imo, callsign, name, draugh, destination, dimension, shipType, aisClass)
        VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s )
        ON CONFLICT (mmsi) 
        DO UPDATE SET 
            imo = COALESCE(EXCLUDED.imo, ais.data.imo),
            callsign = COALESCE(EXCLUDED.callsign, ais.data.callsign),
            name = COALESCE(EXCLUDED.name, ais.data.name),
            draugh = COALESCE(EXCLUDED.draugh, ais.data.draugh),
            destination = COALESCE(EXCLUDED.destination, ais.data.destination),
            dimension = COALESCE(EXCLUDED.dimension, ais.data.dimension),
            shipType = COALESCE(EXCLUDED.shipType, ais.data.shipType);
    """
    values = (
        data.get("mainCalculator").get("mmsi"),
        data.get("mainCalculator").get("messageID"),
        data.get("mainCalculator").get("repeat"),
        data.get("country"),
        data.get("mainCalculator").get("imo"),
        data.get("mainCalculator").get("callsign"),
        data.get("mainCalculator").get("name"),
        data.get("mainCalculator").get("draugh"),
        data.get("mainCalculator").get("destination"),
        data.get("mainCalculator").get("dimension"),
        data.get("mainCalculator").get("shipType"),
        data.get("classType"),
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    print(f"[DB Success] Inserted Data with MMSI: {data.get("mainCalculator").get("mmsi")}")

def StaticReportData(conn, data):
    cursor = conn.cursor()
    query = """
        INSERT INTO ais.data (mmsi, messageID, repeat, flag, name, shipType, dimension, callsign, aisClass)
        VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s)
        ON CONFLICT (mmsi) 
        DO UPDATE SET 
            callsign = COALESCE(EXCLUDED.callsign, ais.data.callsign),
            name = COALESCE(EXCLUDED.name, ais.data.name),
            dimension = COALESCE(EXCLUDED.dimension, ais.data.dimension),
            shipType = COALESCE(EXCLUDED.shipType, ais.data.shipType);
    """
    values = (
        data.get("mainCalculator").get("mmsi"),
        data.get("mainCalculator").get("messageID"),
        data.get("mainCalculator").get("repeat"),
        data.get("country"),
        data.get("mainCalculator").get("name"),
        data.get("mainCalculator").get("shipType"),
        data.get("mainCalculator").get("dimension"),
        data.get("mainCalculator").get("callsign"),
        data.get("classType"),
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    print(f"[DB Success] Inserted Data with MMSI: {data.get("mainCalculator").get("mmsi")}")

def ExtStaticData(conn, data):
    cursor = conn.cursor()
    query = """
        INSERT INTO ais.data (mmsi, messageID, repeat, flag, name, shipType, dimension, aisClass)
        VALUES (%s, %s, %s, %s, %s,%s,%s,%s)
        ON CONFLICT (mmsi) 
        DO UPDATE SET 
            name = COALESCE(EXCLUDED.name, ais.data.name),
            dimension = COALESCE(EXCLUDED.dimension, ais.data.dimension),
            shipType = COALESCE(EXCLUDED.shipType, ais.data.shipType);
    """
    values = (
        data.get("mainCalculator").get("mmsi"),
        data.get("mainCalculator").get("messageID"),
        data.get("mainCalculator").get("repeat"),
        data.get("country"),
        data.get("mainCalculator").get("name"),
        data.get("mainCalculator").get("shipType"),
        data.get("mainCalculator").get("dimension"),
        data.get("classType"),
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    print(f"[DB Success] Inserted Data with MMSI: {data.get("mainCalculator").get("mmsi")}")

def handle_unknown_category(conn, data):
    print(f"[DB Warning] Invalid Data Entry: {data}")

def dataTimeout(conn):
    print("Cleaned DB")
    cursor = conn.cursor()
    query = """
            DELETE from ais.data WHERE updatedat < NOW() - INTERVAL '5 minutes';
        """
    cursor.execute(query)
    conn.commit()

def cleanData(conn):
    cursor = conn.cursor()
    query = """
            DELETE from ais.data;
        """
    cursor.execute(query)
    conn.commit()

CATEGORY_MAP = {
    "Message Type 1": defaultAIS,
    "Message Type 2": defaultAIS,
    "Message Type 3": defaultAIS,
    "Class B position report": defaultAIS,
    "Static Voyage Data": StaticVoyageData,
    "Static Data Report": StaticReportData,
    "Extended Class B position report": ExtStaticData
}

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
        print("Database Connected")
        return conn
    except psycopg.Error as e:
        print("Database connection failed : {e}")
        raise

def main():
    conn = connectDB()
    #create_Table(conn)
    cleanData(conn)
    try:
        for message in consumer:
            payload = message.value
            mainData = payload.get("mainCalculator", {})
            messageID = mainData.get("messageID")
            mmsi = mainData.get("mmsi")

            if not mainData:
                print(f"[Skip] Invalid Data: {payload}")
                continue

            print(f"\n[Inserted] MMSI: {mmsi} | Type: {messageID}")

            handler = CATEGORY_MAP.get(messageID, handle_unknown_category)
            dataTimeout(conn)
            try:
                handler(conn, payload)
            except Exception as db_err:
                print(f"[DB Error] Failed entry {messageID}: {db_err}")
                conn.rollback()

    except KeyboardInterrupt:
        print("\nStopping App...")
    finally:
        consumer.close()
        conn.close()
        print("Application Stopped")

if __name__ == "__main__":
    main()