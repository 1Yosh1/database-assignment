import time
import random
import mysql.connector
import paho.mqtt.client as mqtt
import pymongo
from pymongo import MongoClient

# Sensor Devices MAC Addresses
devices = [
    "b8:27:eb:bf:9d:51",
    "00:0f:00:70:91:0a",
    "1c:bf:ce:15:ec:4d"
]

# Function to generate random sensor data and return it as a dictionary
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

# Function to connect to MySQL database
def connect_to_db():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="12345678",
        database="data_collection"
    )
    print("Connected to MySQL database")
    return mydb

# Function to store data in the MySQL database
def store_data_mysql(data, db_cursor, mydb):
    insert_query = """
    INSERT INTO sensor_readings (ts, device, co, humidity, light, lpg, motion, smoke, temp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
    values = (
        data["ts"], data["device"], data["co"], data["humidity"], data["light"], 
        data["lpg"], data["motion"], data["smoke"], data["temp"]
    )
    db_cursor.execute(insert_query, values)
    mydb.commit()

# Function to connect to MongoDB
def connect_to_mongo():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sensor_data"]
    collection = db["sensor_readings"]
    print("Connected to MongoDB")
    return collection

# Function to store data in the MongoDB
def store_data_mongo(data_list, collection):
    collection.insert_many(data_list)

# MQTT event callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        client.subscribe("sensor/data")
    else:
        print("Connection failed with code", rc)

def on_disconnect(client, userdata, rc):
    print("Disconnected from broker")

def on_message(client, userdata, message):
    print(f"Received message: {message.payload.decode()} on topic {message.topic}")

# Main function
def main():
    # MQTT Broker Connection
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
        mycursor = mydb.cursor()
        mongo_collection = connect_to_mongo()
        
        client.connect(broker_address, broker_port)
        client.loop_start()
        
        data_list = []
        while True:
            data = generate_sensor_data()
            data_list.append(data)
            store_data_mysql(data, mycursor, mydb)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        client.loop_stop()
        client.disconnect()
        mycursor.close()
        mydb.close()
        print("MySQL connection closed")
        
        # Stores the data in MongoDB    
        store_data_mongo(data_list, mongo_collection)
        print("MongoDB connection closed")
if __name__ == "__main__":
    main()
