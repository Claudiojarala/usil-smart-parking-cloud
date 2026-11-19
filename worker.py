import os
import json
from datetime import datetime, timezone
import pymongo
import paho.mqtt.client as mqtt

BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC_EVENTO = "usil/iot/parking/p01/evento"
MONGODB_URI = os.getenv("MONGODB_URI")

if not MONGODB_URI:
    raise RuntimeError("Falta la variable de entorno MONGODB_URI")

client_mongo = pymongo.MongoClient(MONGODB_URI)
db = client_mongo["smart_parking_db"]
collection = db["eventos_parking"]

def on_connect(client, userdata, flags, reason_code, properties=None):
    print("Worker IoT conectado a HiveMQ")
    client.subscribe(TOPIC_EVENTO, qos=0)

def on_message(client, userdata, msg):
    try:
        evento = json.loads(msg.payload.decode("utf-8"))
        evento["sede"] = "p01"
        evento["fecha_hora"] = datetime.now(timezone.utc)
        collection.insert_one(evento)
        print("Evento guardado en MongoDB:", evento)
    except Exception as e:
        print("Error al guardar evento:", e)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="usil-worker-p01")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(BROKER, PORT, 60)
mqtt_client.loop_forever()