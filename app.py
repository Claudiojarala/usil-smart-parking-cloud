import os
import re
import json
import time
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import pymongo
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from google import genai
from google.genai import types

# =======================
# CONFIGURACIÓN
# =======================
st.set_page_config(page_title="Smart Parking Privado USIL", page_icon="🚘", layout="wide")

MONGODB_URI    = os.getenv("MONGODB_URI") or st.secrets.get("app", {}).get("MONGODB_URI", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or st.secrets.get("app", {}).get("GOOGLE_API_KEY", "")

BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC_ESTADO = "usil/iot/parking/p01/estado"
TOPIC_DISPON = "usil/iot/parking/p01/disponibilidad"
TOPIC_CMD    = "usil/iot/parking/p01/cmd"

if not MONGODB_URI or not GOOGLE_API_KEY:
    st.error("❌ Faltan MONGODB_URI o GOOGLE_API_KEY en las variables de entorno.")
    st.stop()

gemini_client = genai.Client(api_key=GOOGLE_API_KEY)
mongo_client = pymongo.MongoClient(MONGODB_URI)
db = mongo_client["smart_parking_db"]
col_eventos = db["eventos_parking"]
col_placas  = db["placas_autorizadas"]

# =======================
# FUNCIONES MQTT, IA Y BD
# =======================
if "estado" not in st.session_state:
    st.session_state.estado = {
        "libres": 5, "ocupadas": 0, "cap": 5,
        "entrada": "CERRADA", "salida": "CERRADA", "rssi": 0
    }
if "disponibilidad" not in st.session_state:
    st.session_state.disponibilidad = "desconocido"

def actualizar_desde_mqtt():
    def on_connect(c, userdata, flags, rc, properties=None):
        c.subscribe([(TOPIC_ESTADO, 0), (TOPIC_DISPON, 1)])

    def on_message(c, userdata, msg):
        texto = msg.payload.decode("utf-8")
        if msg.topic == TOPIC_ESTADO:
            st.session_state.estado = json.loads(texto)
        elif msg.topic == TOPIC_DISPON:
            st.session_state.disponibilidad = texto

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(BROKER, PORT, 60)
    c.loop_start()
    time.sleep(0.6)
    c.loop_stop()
    c.disconnect()

def enviar_comando(comando: str):
    publish.single(TOPIC_CMD, payload=comando, hostname=BROKER, port=PORT, qos=1)

def leer_placa_con_gemini(imagen_bytes: bytes, mime_type: str) -> str:
    prompt = (
        "Extrae únicamente el número de placa vehicular visible en esta imagen. "
        "Normalízala en mayúsculas y con guion (ejemplo: ABC-123). "
        "Si no se distingue ninguna placa, responde exactamente: NO_DETECTADA."
    )
    resp = gemini_client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=imagen_bytes, mime_type=mime_type),
            prompt,
        ],
    )
    texto = (resp.text or "").strip().upper()
    limpio = re.sub(r"[^A-Z0-9-]", "", texto)
    return limpio if limpio else "NO_DETECTADA"

# =======================
# INTERFAZ PRINCIPAL
# =======================
actualizar_desde_mqtt()
est = st.session_state.estado
disp = st.session_state.disponibilidad

st.title("🚘 Control de Acceso Privado – Smart Parking USIL")
st.caption("Reconocimiento de Placas con Cámara Móvil + Gemini AI + MongoDB + ESP32 MQTT")

col_top1, col_top2 = st.columns([3, 1])
with col_top1:
    if disp == "online":
        st.success("🟢 Nodo ESP32: EN LÍNEA (online)")
    else:
        st.error(f"🔴 Nodo ESP32: FUERA DE LÍNEA ({disp})")
with col_top2:
    if st.button("🔄 Actualizar", use_container_width=True):
        st.rerun()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Plazas Libres", f"{est['libres']} / {est['cap']}")
m2.metric("Plazas Ocupadas", est["ocupadas"])
m3.metric("Barrera Entrada", est["entrada"])
m4.metric("Barrera Salida", est["salida"])

tab_camara, tab_placas, tab_control, tab_historial = st.tabs([
    "📷 Escáner de Placas (Acceso)",
    "📋 Placas Autorizadas",
    "🎛️ Control Manual",
    "📊 Histórico MongoDB"
])

# --- TAB 1: ESCÁNER CON CÁMARA DEL CELULAR ---
with tab_camara:
    st.subheader("Escanear placa vehicular para autorizar ingreso")
    foto = st.camera_input("Apunta la cámara de tu celular a la placa del vehículo")

    if foto is not None:
        with st.spinner("Analizando placa con IA y verificando autorización..."):
            placa = leer_placa_con_gemini(foto.getvalue(), foto.type or "image/jpeg")

            if placa == "NO_DETECTADA":
                st.warning("⚠️ No se detectó una placa clara en la foto. Intenta acercar más la cámara.")
            else:
                st.info(f"Placa detectada: **{placa}**")
                registro = col_placas.find_one({"placa": placa, "activo": True})

                if registro and int(est["libres"]) > 0:
                    enviar_comando("ABRIR_ENTRADA")
                    col_eventos.insert_one({
                        "evt": "ACCESO_PLACA_OK",
                        "gate": "ENTRADA",
                        "placa": placa,
                        "propietario": registro.get("propietario", "Autorizado"),
                        "libres": est["libres"],
                        "cap": est["cap"],
                        "sede": "p01",
                        "fecha_hora": datetime.now(timezone.utc),
                    })
                    st.success(f"✅ ACCESO PERMITIDO a **{registro.get('propietario')}** ({placa}). Abriendo barrera de entrada...")
                elif registro and int(est["libres"]) <= 0:
                    st.error(f"⛔ Placa autorizada ({placa}), pero el estacionamiento está LLENO (0 plazas libres).")
                else:
                    col_eventos.insert_one({
                        "evt": "ACCESO_DENEGADO",
                        "gate": "ENTRADA",
                        "placa": placa,
                        "libres": est["libres"],
                        "cap": est["cap"],
                        "sede": "p01",
                        "fecha_hora": datetime.now(timezone.utc),
                    })
                    st.error(f"🚫 ACCESO DENEGADO: La placa **{placa}** no está registrada en el estacionamiento privado.")

# --- TAB 2: GESTIÓN DE LISTA BLANCA ---
with tab_placas:
    st.subheader("Registrar vehículo autorizado")
    with st.form("form_placa", clear_on_submit=True):
        c1, c2 = st.columns(2)
        nueva_placa = c1.text_input("Placa (ej. ABC-123)").strip().upper()
        propietario = c2.text_input("Propietario / Residente (ej. Claudio Jara)")
        if st.form_submit_button("➕ Autorizar placa"):
            if nueva_placa and propietario:
                col_placas.update_one(
                    {"placa": nueva_placa},
                    {"$set": {"placa": nueva_placa, "propietario": propietario, "activo": True}},
                    upsert=True,
                )
                st.success(f"Placa {nueva_placa} autorizada correctamente.")
                st.rerun()

    autorizadas = list(col_placas.find({}, {"_id": 0}))
    if autorizadas:
        st.dataframe(pd.DataFrame(autorizadas), use_container_width=True, hide_index=True)
    else:
        st.info("No hay placas registradas todavía. Agrega la tuya arriba para probar.")

# --- TAB 3: CONTROL MANUAL ---
with tab_control:
    st.subheader("Comandos remotos directos al ESP32")
    b1, b2, b3, b4 = st.columns(4)
    with b1:
        if st.button("⬆️ Abrir Entrada", use_container_width=True):
            enviar_comando("ABRIR_ENTRADA")
            st.toast("Comando ABRIR_ENTRADA enviado")
    with b2:
        if st.button("⬆️ Abrir Salida", use_container_width=True):
            enviar_comando("ABRIR_SALIDA")
            st.toast("Comando ABRIR_SALIDA enviado")
    with b3:
        if st.button("📡 PING", use_container_width=True):
            enviar_comando("PING")
            st.toast("PING enviado")
    with b4:
        cupo = st.number_input("Cupos:", 0, 5, int(est["libres"]), label_visibility="collapsed")
        if st.button("💾 SET_LIBRES", use_container_width=True):
            enviar_comando(f"SET_LIBRES:{cupo}")
            st.toast(f"SET_LIBRES:{cupo} enviado")

# --- TAB 4: HISTORIAL ---
with tab_historial:
    docs = list(col_eventos.find({}, {"_id": 0}).sort("fecha_hora", -1).limit(50))
    if docs:
        st.dataframe(pd.DataFrame(docs), use_container_width=True, hide_index=True)
    else:
        st.info("Aún no hay eventos registrados.")