import os
import re
import json
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st
import pymongo
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from google import genai
from google.genai import types

# ====================
# CONFIGURACIÓN DE PÁGINA
# ====================
st.set_page_config(
    page_title="ParkGate AI · Control de Acceso",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

MONGODB_URI    = os.getenv("MONGODB_URI") or st.secrets.get("app", {}).get("MONGODB_URI", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or st.secrets.get("app", {}).get("GOOGLE_API_KEY", "")

BROKER        = "broker.hivemq.com"
PORT          = 1883
TOPIC_ESTADO  = "usil/iot/parking/p01/estado"
TOPIC_DISPON  = "usil/iot/parking/p01/disponibilidad"
TOPIC_CMD     = "usil/iot/parking/p01/cmd"

TZ_LIMA = timezone(timedelta(hours=-5))

# ====================
# CSS DE PRODUCCIÓN (SAAS / CONTROL CENTER)
# ====================
st.markdown("""
<style>
    /* Ocultar marca de agua y espacios innecesarios */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2.5rem;
        max-width: 1180px;
    }

    /* Header principal tipo SaaS */
    .hero-header {
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
        border: 1px solid #334155;
        border-radius: 16px;
        padding: 1.35rem 1.6rem;
        margin-bottom: 1.25rem;
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        align-items: center;
        gap: 1rem;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.4);
    }
    .hero-title {
        font-size: 1.55rem;
        font-weight: 700;
        color: #F8FAFC;
        margin: 0;
        letter-spacing: -0.02em;
    }
    .hero-sub {
        font-size: 0.88rem;
        color: #94A3B8;
        margin: 0.25rem 0 0 0;
    }

    /* Badges de estado IoT */
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.45rem 0.95rem;
        border-radius: 9999px;
        font-size: 0.82rem;
        font-weight: 600;
        letter-spacing: 0.03em;
    }
    .status-online {
        background: rgba(16, 185, 129, 0.14);
        color: #34D399;
        border: 1px solid rgba(16, 185, 129, 0.4);
    }
    .status-offline {
        background: rgba(244, 63, 94, 0.14);
        color: #FB7185;
        border: 1px solid rgba(244, 63, 94, 0.4);
    }
    .dot-online {
        width: 8px; height: 8px; border-radius: 50%;
        background: #10B981;
        box-shadow: 0 0 8px #10B981;
    }
    .dot-offline {
        width: 8px; height: 8px; border-radius: 50%;
        background: #F43F5E;
    }

    /* Tarjetas KPI */
    .kpi-card {
        background: #111827;
        border: 1px solid #1F2937;
        border-radius: 14px;
        padding: 1.1rem 1.25rem;
        transition: border-color 0.2s ease;
    }
    .kpi-card:hover {
        border-color: #3B82F6;
    }
    .kpi-label {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94A3B8;
        font-weight: 600;
        margin-bottom: 0.35rem;
    }
    .kpi-value {
        font-size: 1.85rem;
        font-weight: 700;
        color: #F8FAFC;
        font-variant-numeric: tabular-nums;
        line-height: 1.15;
    }
    .kpi-foot {
        font-size: 0.78rem;
        color: #64748B;
        margin-top: 0.35rem;
    }

    /* Ticket digital de resultado LPR */
    .plate-ticket-ok {
        background: linear-gradient(145deg, rgba(6, 78, 59, 0.45), rgba(17, 24, 39, 0.95));
        border: 1px solid #10B981;
        border-left: 6px solid #10B981;
        border-radius: 14px;
        padding: 1.25rem 1.4rem;
        margin-top: 0.75rem;
    }
    .plate-ticket-deny {
        background: linear-gradient(145deg, rgba(136, 19, 55, 0.45), rgba(17, 24, 39, 0.95));
        border: 1px solid #F43F5E;
        border-left: 6px solid #F43F5E;
        border-radius: 14px;
        padding: 1.25rem 1.4rem;
        margin-top: 0.75rem;
    }
    .plate-badge {
        display: inline-block;
        background: #F8FAFC;
        color: #0F172A;
        font-family: 'Courier New', monospace;
        font-weight: 800;
        font-size: 1.55rem;
        letter-spacing: 0.12em;
        padding: 0.25rem 0.85rem;
        border-radius: 8px;
        border: 2px solid #0F172A;
        box-shadow: 0 2px 6px rgba(0,0,0,0.35);
        margin: 0.4rem 0;
    }

    /* Tabs y botones más limpios */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #111827;
        padding: 6px;
        border-radius: 12px;
        border: 1px solid #1F2937;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 16px;
        font-weight: 600;
    }
    .stButton > button {
        border-radius: 10px;
        font-weight: 600;
        transition: all 0.15s ease;
    }
</style>
""", unsafe_allow_html=True)

# ====================
# CONEXIÓN MONGODB Y GEMINI
# ====================
@st.cache_resource
def get_db():
    if not MONGODB_URI:
        return None
    client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    return client["smart_parking_db"]

@st.cache_resource
def get_gemini():
    if not GOOGLE_API_KEY:
        return None
    return genai.Client(api_key=GOOGLE_API_KEY)

db = get_db()
gemini_client = get_gemini()

# ====================
# FUNCIONES DE APOYO
# ====================
def obtener_estado_mqtt():
    datos = {"estado": None, "disponibilidad": "desconocido"}
    def on_connect(c, u, f, rc):
        if rc == 0:
            c.subscribe(TOPIC_ESTADO)
            c.subscribe(TOPIC_DISPON)
    def on_message(c, u, msg):
        payload = msg.payload.decode("utf-8", errors="ignore")
        if msg.topic == TOPIC_ESTADO:
            try:
                datos["estado"] = json.loads(payload)
            except Exception:
                pass
        elif msg.topic == TOPIC_DISPON:
            datos["disponibilidad"] = payload

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(BROKER, PORT, 15)
        client.loop_start()
        time.sleep(0.9)
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    return datos

def enviar_comando_esp32(cmd: str):
    try:
        publish.single(TOPIC_CMD, payload=cmd, hostname=BROKER, port=PORT)
        return True
    except Exception as e:
        st.error(f"Error enviando comando MQTT: {e}")
        return False

def normalizar_placa(texto: str) -> str:
    limpio = re.sub(r"[^A-Z0-9]", "", texto.upper())
    if len(limpio) == 6:
        return f"{limpio[:3]}-{limpio[3:]}"
    return limpio

def reconocer_placa_gemini(imagen_bytes: bytes, mime_type: str) -> str:
    if not gemini_client:
        return "SIN_API_KEY"
    prompt = (
        "Extrae únicamente el número de placa vehicular visible en la imagen. "
        "Responde SOLO con los caracteres alfanuméricos de la placa (ejemplo: ABC123 o ABC-123), "
        "sin texto adicional ni explicaciones. Si no hay ninguna placa legible, responde SIN_PLACA."
    )
    response = gemini_client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            types.Part.from_bytes(data=imagen_bytes, mime_type=mime_type),
            prompt
        ]
    )
    return response.text.strip()

def registrar_evento_db(evento: str, placa: str = "", detalle: str = ""):
    if db is not None:
        db["eventos_parking"].insert_one({
            "evento": evento,
            "placa": placa,
            "detalle": detalle,
            "origen": "WEB_LPR",
            "fecha_utc": datetime.now(timezone.utc)
        })

# ====================
# TELEMETRÍA EN VIVO Y HERO HEADER
# ====================
mqtt_data = obtener_estado_mqtt()
est = mqtt_data["estado"] or {}
disp = mqtt_data["disponibilidad"]

libres   = int(est.get("libres", 5))
ocupados = int(est.get("ocupados", 0))
cap      = int(est.get("cap", 5))
b_ent    = est.get("barrera_entrada", "CERRADA")
b_sal    = est.get("barrera_salida", "CERRADA")
es_online = (disp == "online")

badge_html = (
    '<span class="status-pill status-online"><span class="dot-online"></span>NODO ESP32 · EN LÍNEA</span>'
    if es_online else
    '<span class="status-pill status-offline"><span class="dot-offline"></span>NODO ESP32 · ESPERANDO FIRMWARE</span>'
)

st.markdown(f"""
<div class="hero-header">
    <div>
        <div class="hero-title">🛡️ ParkGate AI · Control de Acceso Privado</div>
        <div class="hero-sub">Visión Artificial LPR (Gemini 2.5 Flash) · Telemetría MQTT · Persistencia MongoDB Atlas</div>
    </div>
    <div>{badge_html}</div>
</div>
""", unsafe_allow_html=True)

# Barra de KPIs
c1, c2, c3, c4 = st.columns(4)
pct_ocupacion = int((ocupados / cap) * 100) if cap > 0 else 0
color_ent = "#10B981" if b_ent == "ABIERTA" else "#94A3B8"
color_sal = "#10B981" if b_sal == "ABIERTA" else "#94A3B8"

with c1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Plazas Disponibles</div>
        <div class="kpi-value">{libres} <span style="font-size:1rem;color:#64748B;">/ {cap}</span></div>
        <div class="kpi-foot">Capacidad total habilitada</div>
    </div>""", unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Ocupación Actual</div>
        <div class="kpi-value">{ocupados} <span style="font-size:1rem;color:#3B82F6;">({pct_ocupacion}%)</span></div>
        <div class="kpi-foot">Vehículos dentro del recinto</div>
    </div>""", unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Barrera de Ingreso</div>
        <div class="kpi-value" style="color:{color_ent};font-size:1.55rem;">{b_ent}</div>
        <div class="kpi-foot">Canal MQTT · p01/entrada</div>
    </div>""", unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Barrera de Salida</div>
        <div class="kpi-value" style="color:{color_sal};font-size:1.55rem;">{b_sal}</div>
        <div class="kpi-foot">Canal MQTT · p01/salida</div>
    </div>""", unsafe_allow_html=True)

st.write("")

# ====================
# NAVEGACIÓN PRINCIPAL
# ====================
tab_cam, tab_placas, tab_ctrl, tab_hist = st.tabs([
    "📷 Escáner LPR",
    "🪪 Padrón Autorizado",
    "🎛️ Consola de Barreras",
    "📊 Auditoría & Logs"
])

# ---------------------------------------------------------
# TAB 1: ESCÁNER DE PLACAS CON CÁMARA MÓVIL + GEMINI
# ---------------------------------------------------------
with tab_cam:
    col_izq, col_der = st.columns([1.15, 1], gap="large")

    with col_izq:
        with st.container(border=True):
            st.markdown("#### Captura Óptica Vehicular")
            st.caption("Enfoca la placa delantera o trasera del vehículo con buena iluminación.")
            foto = st.camera_input("Cámara de control de acceso", label_visibility="collapsed")

    with col_der:
        with st.container(border=True):
            st.markdown("#### Dictamen de Control de Acceso")
            if foto is not None:
                with st.spinner("Procesando imagen con Gemini 2.5 Flash..."):
                    raw_texto = reconocer_placa_gemini(foto.getvalue(), foto.type)
                    placa = normalizar_placa(raw_texto)

                if not placa or "SINPLACA" in placa:
                    st.warning("No se detectó una matrícula legible. Acerca un poco más la cámara e inténtalo de nuevo.")
                elif db is None:
                    st.error("Falta configurar la conexión a MongoDB Atlas.")
                else:
                    hora_lima = datetime.now(TZ_LIMA).strftime("%d/%m/%Y · %H:%M:%S")
                    registro = db["placas_autorizadas"].find_one({"placa": placa, "activo": True})

                    if registro:
                        propietario = registro.get("propietario", "Residente Autorizado")
                        if libres <= 0:
                            st.markdown(f"""
                            <div class="plate-ticket-deny">
                                <div style="font-size:0.8rem;font-weight:700;color:#FBBF24;letter-spacing:0.06em;">⚠️ ESTACIONAMIENTO COMPLETO</div>
                                <div class="plate-badge">{placa}</div>
                                <div style="color:#E2E8F0;margin-top:0.35rem;">Titular: <b>{propietario}</b></div>
                                <div style="color:#94A3B8;font-size:0.82rem;margin-top:0.25rem;">Sin plazas disponibles ({hora_lima})</div>
                            </div>
                            """, unsafe_allow_html=True)
                            registrar_evento_db("ACCESO_DENEGADO_LLENO", placa, propietario)
                        else:
                            enviar_comando_esp32("ABRIR_ENTRADA")
                            registrar_evento_db("ACCESO_PLACA_OK", placa, f"Autorizado: {propietario}")
                            st.markdown(f"""
                            <div class="plate-ticket-ok">
                                <div style="font-size:0.8rem;font-weight:700;color:#34D399;letter-spacing:0.06em;">✅ ACCESO VEHICULAR AUTORIZADO</div>
                                <div class="plate-badge">{placa}</div>
                                <div style="color:#F8FAFC;margin-top:0.35rem;">Titular: <b>{propietario}</b></div>
                                <div style="color:#A7F3D0;font-size:0.83rem;margin-top:0.3rem;">Orden <code>ABRIR_ENTRADA</code> enviada al ESP32 · {hora_lima}</div>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        registrar_evento_db("ACCESO_DENEGADO", placa, "Placa no autorizada")
                        st.markdown(f"""
                        <div class="plate-ticket-deny">
                            <div style="font-size:0.8rem;font-weight:700;color:#FB7185;letter-spacing:0.06em;">🚫 ACCESO DENEGADO · NO REGISTRADA</div>
                            <div class="plate-badge">{placa}</div>
                            <div style="color:#FECDD3;margin-top:0.35rem;">La matrícula no figura en la lista blanca del recinto privado.</div>
                            <div style="color:#94A3B8;font-size:0.82rem;margin-top:0.25rem;">Incidente registrado en auditoría · {hora_lima}</div>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.info("Esperando captura de cámara. Al tomar la fotografía se validará la matrícula automáticamente contra MongoDB Atlas.")

# ---------------------------------------------------------
# TAB 2: GESTIÓN DE PLACAS AUTORIZADAS
# ---------------------------------------------------------
with tab_placas:
    c_form, c_list = st.columns([1, 1.45], gap="large")

    with c_form:
        with st.container(border=True):
            st.markdown("#### Registrar Vehículo")
            st.caption("Agrega o actualiza matrículas habilitadas para apertura automática.")
            with st.form("form_placa", clear_on_submit=True):
                nueva_placa = st.text_input("Matrícula", placeholder="Ej. BBW-266")
                propietario = st.text_input("Propietario / Unidad", placeholder="Ej. Claudio Jara · Dpto 402")
                guardado = st.form_submit_button("Autorizar Matrícula", type="primary", use_container_width=True)
                if guardado and nueva_placa.strip():
                    p_norm = normalizar_placa(nueva_placa)
                    if db is not None:
                        db["placas_autorizadas"].update_one(
                            {"placa": p_norm},
                            {"$set": {
                                "placa": p_norm,
                                "propietario": propietario.strip() or "Sin especificar",
                                "activo": True,
                                "actualizado_utc": datetime.now(timezone.utc)
                            }},
                            upsert=True
                        )
                        st.success(f"Matrícula **{p_norm}** habilitada.")
                        st.rerun()

    with c_list:
        with st.container(border=True):
            st.markdown("#### Vehículos Habilitados")
            if db is not None:
                lista = list(db["placas_autorizadas"].find({"activo": True}, {"_id": 0, "placa": 1, "propietario": 1}))
                if lista:
                    st.dataframe(pd.DataFrame(lista), use_container_width=True, hide_index=True)
                    col_del1, col_del2 = st.columns([2, 1])
                    with col_del1:
                        placa_borrar = st.selectbox(
                            "Revocar acceso",
                            [x["placa"] for x in lista],
                            label_visibility="collapsed"
                        )
                    with col_del2:
                        if st.button("Revocar", use_container_width=True):
                            db["placas_autorizadas"].update_one({"placa": placa_borrar}, {"$set": {"activo": False}})
                            st.rerun()
                else:
                    st.info("Aún no hay vehículos registrados en el padrón.")

# ---------------------------------------------------------
# TAB 3: CONTROL MANUAL DE BARRERAS
# ---------------------------------------------------------
with tab_ctrl:
    col_m1, col_m2 = st.columns(2, gap="large")

    with col_m1:
        with st.container(border=True):
            st.markdown("#### 🚧 Barrera de Entrada (P01)")
            st.caption("Accionamiento remoto directo sobre el servomotor de ingreso.")
            b1, b2 = st.columns(2)
            with b1:
                if st.button("⬆️ Abrir Entrada", type="primary", use_container_width=True):
                    if enviar_comando_esp32("ABRIR_ENTRADA"):
                        registrar_evento_db("APERTURA_MANUAL_ENTRADA", "", "Consola operador")
                        st.toast("Comando ABRIR_ENTRADA enviado", icon="✅")
            with b2:
                if st.button("⬇️ Cerrar Entrada", use_container_width=True):
                    if enviar_comando_esp32("CERRAR_ENTRADA"):
                        st.toast("Comando CERRAR_ENTRADA enviado", icon="🔒")

    with col_m2:
        with st.container(border=True):
            st.markdown("#### 🚧 Barrera de Salida (P01)")
            st.caption("Accionamiento remoto directo sobre el servomotor de salida.")
            b3, b4 = st.columns(2)
            with b3:
                if st.button("⬆️ Abrir Salida", type="primary", use_container_width=True):
                    if enviar_comando_esp32("ABRIR_SALIDA"):
                        registrar_evento_db("APERTURA_MANUAL_SALIDA", "", "Consola operador")
                        st.toast("Comando ABRIR_SALIDA enviado", icon="✅")
            with b4:
                if st.button("⬇️ Cerrar Salida", use_container_width=True):
                    if enviar_comando_esp32("CERRAR_SALIDA"):
                        st.toast("Comando CERRAR_SALIDA enviado", icon="🔒")

    st.write("")
    if st.button("🔄 Sincronizar Telemetría MQTT", use_container_width=True):
        st.rerun()

# ---------------------------------------------------------
# TAB 4: HISTÓRICO EN MONGODB ATLAS
# ---------------------------------------------------------
with tab_hist:
    with st.container(border=True):
        h_top1, h_top2 = st.columns([3, 1])
        with h_top1:
            st.markdown("#### Bitácora de Accesos e Incidentes")
            st.caption("Registros persistidos en tiempo real en MongoDB Atlas (Hora Perú UTC-5).")
        with h_top2:
            if st.button("🔄 Refrescar Logs", use_container_width=True):
                st.rerun()

        if db is not None:
            eventos = list(db["eventos_parking"].find({}, {"_id": 0}).sort("fecha_utc", -1).limit(50))
            if eventos:
                df = pd.DataFrame(eventos)
                if "fecha_utc" in df.columns:
                    df["Hora Lima"] = pd.to_datetime(df["fecha_utc"], utc=True).dt.tz_convert(TZ_LIMA).dt.strftime("%d/%m/%Y %H:%M:%S")
                    df = df.drop(columns=["fecha_utc"])
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("No hay eventos registrados todavía.")