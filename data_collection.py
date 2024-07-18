import time
import random
import mysql.connector
import paho.mqtt.client as mqtt
import pymongo
from neo4j import GraphDatabase

# Sensor Devices MAC Addresses
devices = [
    "b8:27:eb:bf:9d:51",
    "00:0f:00:70:91:0a",
    "1c:bf:ce:15:ec:4d"
]

# Example user data
users = [
    ("user1", "password1"),
    ("user2", "password2"),
    ("user3", "password3")
]

def generate_sensor_data():
    ts = time.time()
    device = random.choice(devices)
    co = round(random.uniform(0.001, 0.01), 10)
    humidity = round(random.uniform(30.0, 80.0), 1)
    light = random.choice([True, False])
    lpg = round(random.uniform(0.001, 0.01), 10)
    motion = random.choice([True, False])
    smoke = round(random.uniform(0.01, 0.03), 10)
    temp = round(random.uniform(15.0, 30.0), 1)
    return {
        "ts": ts,
        "device": device,
        "co": co,
        "humidity": humidity,
        "light": light,
        "lpg": lpg,
        "motion": motion,
        "smoke": smoke,
        "temp": temp
    }

def connect_to_db():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="sensor_user",
            password="sensor_password",
            database="data_collection"
        )
        print("Connected to MySQL database")
        return mydb
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def store_data_mysql(data, db_cursor, mydb):
    try:
        insert_query = """
        INSERT INTO sensor_readings (ts, device_id, co, humidity, light, lpg, motion, smoke, temp)
        VALUES (%s, (SELECT device_id FROM devices WHERE device_mac = %s), %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            data["ts"], data["device"], data["co"], data["humidity"], data["light"],
            data["lpg"], data["motion"], data["smoke"], data["temp"]
        )
        db_cursor.execute(insert_query, values)
        mydb.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

def connect_to_mongo():
    try:
        client = MongoClient("mongodb://sensor_user:sensor_password@localhost:27017/")
        db = client["sensor_data"]
        collection = db["sensor_readings"]
        print("Connected to MongoDB")
        return collection
    except pymongo.errors.ConnectionError as err:
        print(f"Error: {err}")
        return None

def store_data_mongo(data_list, collection): 
    try:
        collection.insert_many(data_list)
    except pymongo.errors.BulkWriteError as err:
        print(f"Error: {err}")

def connect_to_neo4j():
    uri = "neo4j://localhost:7687"
    username = "eyoas"
    password = "sensor_password"
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        print("Connected to Neo4j")
        return driver
    except Exception as e:
        print(f"Error: {e}")
        return None

def store_data_neo4j(driver, data):
    try:
        with driver.session() as session:
            device_mac = data["device"]
            ts = data["ts"]
            co = data["co"]
            humidity = data["humidity"]
            light = data["light"]
            lpg = data["lpg"]
            motion = data["motion"]
            smoke = data["smoke"]
            temp = data["temp"]
            session.write_transaction(create_sensor_reading, device_mac, ts, co, humidity, light, lpg, motion, smoke, temp)
    except Exception as e:
        print(f"Error: {e}")

def create_sensor_reading(tx, device_mac, ts, co, humidity, light, lpg, motion, smoke, temp):
    tx.run("""
    MERGE (d:Device {mac: $device_mac})
    CREATE (r:Reading {ts: $ts, co: $co, humidity: $humidity, light: $light, lpg: $lpg, motion: $motion, smoke: $smoke, temp: $temp})
    MERGE (d)-[:HAS_READING]->(r)
    """, device_mac=device_mac, ts=ts, co=co, humidity=humidity, light=light, lpg=lpg, motion=motion, smoke=smoke, temp=temp)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        client.subscribe("#")  # Subscribe to all topics
    else:
        print("Connection failed with code", rc)

def on_disconnect(client, userdata, rc):
    print("Disconnected from broker")

def on_message(client, userdata, message):
    print(f"Received message: {message.payload.decode()} on topic {message.topic}")

def setup_users_devices(mydb):
    try:
        cursor = mydb.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(50) NOT NULL UNIQUE, password VARCHAR(255) NOT NULL)")
        cursor.executemany("INSERT IGNORE INTO users (username, password) VALUES (%s, %s)", users)
        cursor.execute("CREATE TABLE IF NOT EXISTS devices (device_id INT AUTO_INCREMENT PRIMARY KEY, device_mac VARCHAR(17) NOT NULL UNIQUE, user_id INT, FOREIGN KEY (user_id) REFERENCES users(user_id))")
        for device in devices:
            cursor.execute("INSERT IGNORE INTO devices (device_mac, user_id) VALUES (%s, (SELECT user_id FROM users ORDER BY RAND() LIMIT 1))", (device,))
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            reading_id INT AUTO_INCREMENT PRIMARY KEY,
            ts BIGINT NOT NULL,
            device_id INT,
            co DECIMAL(12, 10) NOT NULL,
            humidity DECIMAL(5, 1) NOT NULL,
            light BOOLEAN NOT NULL,
            lpg DECIMAL(12, 10) NOT NULL,
            motion BOOLEAN NOT NULL,
            smoke DECIMAL(12, 10) NOT NULL,
            temp DECIMAL(4, 1) NOT NULL,
            FOREIGN KEY (device_id) REFERENCES devices(device_id)
        )
        """)
        mydb.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

def subscribe_to_topics(client):
    client.subscribe("#")  # Subscribe to all topics

def publish_message(client, topic, message):
    client.publish(topic, message)

def main():
    broker_address = "76fe501d8d0e4dc3a8c6c8035fdc9ff0.s1.eu.hivemq.cloud"
    broker_port = 8883

    client = mqtt.Client()
    client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLS)
    client.username_pw_set("eyoas", "Password1!")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    try:
        mydb = connect_to_db()
        if mydb is None:
            return
        setup_users_devices(mydb)
        mycursor = mydb.cursor()
        mongo_collection = connect_to_mongo()
        neo4j_driver = connect_to_neo4j()

        client.connect(broker_address, broker_port)
        client.loop_start()

        data_list = []
        while True:
            data = generate_sensor_data()
            data_list.append(data)
            store_data_mysql(data, mycursor, mydb)
            store_data_neo4j(neo4j_driver, data)

            publish_message(client, "sensor/temp", str(data["temp"]))
            publish_message(client, "sensor/co", str(data["co"]))
            publish_message(client, "sensor/humidity", str(data["humidity"]))
            publish_message(client, "sensor/light", str(data["light"]))
            publish_message(client, "sensor/lpg", str(data["lpg"]))
            publish_message(client, "sensor/motion", str(data["motion"]))
            publish_message(client, "sensor/smoke", str(data["smoke"]))

            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        client.loop_stop()
        client.disconnect()
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()
        print("MySQL connection closed")
        store_data_mongo(data_list, mongo_collection)
        print("MongoDB connection closed")
        neo4j_driver.close()
        print("Neo4j connection closed")

if __name__ == "__main__":
    main()