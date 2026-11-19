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
    page_title="ParkGate AI · Smart Parking USIL",
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
TOPIC_LCD     = "usil/iot/parking/p01/lcd"

TZ_LIMA = timezone(timedelta(hours=-5))

# ====================
# ESTILOS CSS DE PRODUCCIÓN (PORTADOS DE PARKGATE-AI)
# ====================
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .block-container {
        padding-top: 1.35rem;
        padding-bottom: 2.5rem;
        max-width: 1220px;
    }

    /* Hero Header */
    .hero-header {
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
        border: 1px solid #334155;
        border-radius: 16px;
        padding: 1.25rem 1.5rem;
        margin-bottom: 1rem;
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        align-items: center;
        gap: 1rem;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.45);
    }
    .hero-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #F8FAFC;
        margin: 0;
        letter-spacing: -0.02em;
    }
    .hero-sub {
        font-size: 0.86rem;
        color: #94A3B8;
        margin: 0.25rem 0 0 0;
    }

    /* Badges IoT */
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.42rem 0.9rem;
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 600;
        letter-spacing: 0.03em;
    }
    .status-online {
        background: rgba(16, 185, 129, 0.15);
        color: #34D399;
        border: 1px solid rgba(16, 185, 129, 0.4);
    }
    .status-offline {
        background: rgba(244, 63, 94, 0.15);
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

    /* Alert Strip */
    .alert-banner {
        background: rgba(245, 158, 11, 0.12);
        border: 1px solid rgba(245, 158, 11, 0.45);
        border-left: 5px solid #F59E0B;
        border-radius: 12px;
        padding: 0.75rem 1.1rem;
        margin-bottom: 1rem;
        color: #FDE68A;
        font-size: 0.88rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    /* KPI Strip */
    .kpi-card {
        background: #111827;
        border: 1px solid #1F2937;
        border-radius: 14px;
        padding: 1.05rem 1.2rem;
        transition: border-color 0.2s ease;
    }
    .kpi-card:hover {
        border-color: #3B82F6;
    }
    .kpi-label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94A3B8;
        font-weight: 600;
        margin-bottom: 0.3rem;
    }
    .kpi-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #F8FAFC;
        font-variant-numeric: tabular-nums;
        line-height: 1.15;
    }
    .kpi-foot {
        font-size: 0.76rem;
        color: #64748B;
        margin-top: 0.3rem;
    }

    /* Space Monitor Cards (Espacio 1 y 2) */
    .space-card-free {
        background: linear-gradient(145deg, rgba(6, 78, 59, 0.3), #111827);
        border: 1px solid rgba(16, 185, 129, 0.45);
        border-radius: 14px;
        padding: 1.1rem 1.25rem;
    }
    .space-card-busy {
        background: linear-gradient(145deg, rgba(136, 19, 55, 0.3), #111827);
        border: 1px solid rgba(244, 63, 94, 0.45);
        border-radius: 14px;
        padding: 1.1rem 1.25rem;
    }

    /* Ticket digital LPR */
    .ticket-ok {
        background: linear-gradient(145deg, rgba(6, 78, 59, 0.48), rgba(17, 24, 39, 0.96));
        border: 1px solid #10B981;
        border-left: 6px solid #10B981;
        border-radius: 14px;
        padding: 1.2rem 1.35rem;
        margin-top: 0.65rem;
    }
    .ticket-warn {
        background: linear-gradient(145deg, rgba(146, 64, 14, 0.48), rgba(17, 24, 39, 0.96));
        border: 1px solid #F59E0B;
        border-left: 6px solid #F59E0B;
        border-radius: 14px;
        padding: 1.2rem 1.35rem;
        margin-top: 0.65rem;
    }
    .ticket-deny {
        background: linear-gradient(145deg, rgba(136, 19, 55, 0.48), rgba(17, 24, 39, 0.96));
        border: 1px solid #F43F5E;
        border-left: 6px solid #F43F5E;
        border-radius: 14px;
        padding: 1.2rem 1.35rem;
        margin-top: 0.65rem;
    }
    .plate-badge {
        display: inline-block;
        background: #F8FAFC;
        color: #0F172A;
        font-family: 'Courier New', monospace;
        font-weight: 800;
        font-size: 1.5rem;
        letter-spacing: 0.12em;
        padding: 0.22rem 0.8rem;
        border-radius: 8px;
        border: 2px solid #0F172A;
        box-shadow: 0 2px 6px rgba(0,0,0,0.35);
        margin: 0.4rem 0;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 6px;
        background-color: #111827;
        padding: 6px;
        border-radius: 12px;
        border: 1px solid #1F2937;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 14px;
        font-weight: 600;
    }
    .stButton > button {
        border-radius: 10px;
        font-weight: 600;
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
# INICIALIZACIÓN DE ESTADO EN MONGODB (CAP 5, 3 OCUPADOS, 2 LIBRES)
# ====================
def asegurar_estado_inicial():
    if db is None:
        return {
            "capacidad": 5, "ocupados_base": 3, "espacio_1_ocupado": False,
            "espacio_2_ocupado": False, "led_verde_1": True, "led_verde_2": True,
            "led_rojo_entrada": True, "led_rojo_salida": True,
            "lcd_linea_1": "SMART PARKING USIL", "lcd_linea_2": "Libres: 2 / 5"
        }
    estado = db["estado_sistema"].find_one({"_id": "p01"})
    if not estado:
        estado = {
            "_id": "p01",
            "capacidad": 5,
            "ocupados_base": 3,
            "espacio_1_ocupado": False,
            "espacio_2_ocupado": False,
            "led_verde_1": True,
            "led_verde_2": True,
            "led_rojo_entrada": True,
            "led_rojo_salida": True,
            "lcd_linea_1": "SMART PARKING USIL",
            "lcd_linea_2": "Libres: 2 / 5"
        }
        db["estado_sistema"].insert_one(estado)
    return estado

estado_db = asegurar_estado_inicial()

# ====================
# FUNCIONES DE APOYO (MQTT, LPR, AUDITORÍA)
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
        time.sleep(0.85)
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
    return datos

def enviar_comando_esp32(cmd: str, topic: str = TOPIC_CMD):
    try:
        publish.single(topic, payload=cmd, hostname=BROKER, port=PORT)
        return True
    except Exception as e:
        st.error(f"Error enviando comando MQTT: {e}")
        return False

def normalizar_placa(texto: str) -> str:
    limpio = re.sub(r"[^A-Z0-9]", "", texto.upper())
    if len(limpio) == 6:
        return f"{limpio[:3]}-{limpio[3:]}"
    return limpio

def reconocer_placa_gemini(imagen_bytes: bytes, mime_type: str) -> tuple[str, float]:
    inicio = time.perf_counter()
    if not gemini_client:
        return "SIN_API_KEY", 0.0
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
    latencia = round(time.perf_counter() - inicio, 2)
    return response.text.strip(), latencia

def registrar_evento_db(evento: str, placa: str = "", detalle: str = "", severidad: str = "INFO"):
    if db is not None:
        db["eventos_parking"].insert_one({
            "evento": evento,
            "placa": placa,
            "detalle": detalle,
            "severidad": severidad,
            "fecha_utc": datetime.now(timezone.utc)
        })

# ====================
# CÁLCULO DE TELEMETRÍA UNIFICADA (MQTT + MONGODB)
# ====================
mqtt_data = obtener_estado_mqtt()
est_mqtt = mqtt_data["estado"] or {}
disp = mqtt_data["disponibilidad"]
es_online = (disp == "online")

cap = int(est_mqtt.get("cap", estado_db.get("capacidad", 5)))
esp1_ocupado = bool(est_mqtt.get("espacio_1", estado_db.get("espacio_1_ocupado", False)))
esp2_ocupado = bool(est_mqtt.get("espacio_2", estado_db.get("espacio_2_ocupado", False)))

# 3 ocupados fijos de simulación + estado real de Espacio 1 y Espacio 2
ocupados_base = int(estado_db.get("ocupados_base", 3))
ocupados = min(cap, ocupados_base + (1 if esp1_ocupado else 0) + (1 if esp2_ocupado else 0))
libres = max(0, cap - ocupados)

b_ent = est_mqtt.get("barrera_entrada", "CERRADA")
b_sal = est_mqtt.get("barrera_salida", "CERRADA")

# ====================
# HERO HEADER & ALERT STRIP
# ====================
badge_html = (
    '<span class="status-pill status-online"><span class="dot-online"></span>NODO ESP32 · EN LÍNEA</span>'
    if es_online else
    '<span class="status-pill status-offline"><span class="dot-offline"></span>NODO ESP32 · ESPERANDO FIRMWARE</span>'
)

st.markdown(f"""
<div class="hero-header">
    <div>
        <div class="hero-title">🛡️ ParkGate AI · Control de Acceso Privado USIL</div>
        <div class="hero-sub">Visión LPR (Gemini 2.5 Flash) · Anti-Clonación · Monitor de Plazas · Teleoperación MQTT</div>
    </div>
    <div>{badge_html}</div>
</div>
""", unsafe_allow_html=True)

# Alerta reciente de seguridad (si hubo clonación o invasión de plaza en últimos registros)
if db is not None:
    ultima_alerta = db["eventos_parking"].find_one({"severidad": "ALERTA"}, sort=[("fecha_utc", -1)])
    if ultima_alerta:
        hora_al = pd.to_datetime(ultima_alerta["fecha_utc"], utc=True).tz_convert(TZ_LIMA).strftime("%H:%M:%S")
        st.markdown(f"""
        <div class="alert-banner">
            <div>⚠️ <b>ÚLTIMA ALERTA DE SEGURIDAD ({hora_al}):</b> {ultima_alerta.get("evento")} — Placa <code>{ultima_alerta.get("placa", "N/A")}</code> ({ultima_alerta.get("detalle", "")})</div>
        </div>
        """, unsafe_allow_html=True)

# ====================
# KPI STRIP
# ====================
c1, c2, c3, c4 = st.columns(4)
pct_ocupacion = int((ocupados / cap) * 100) if cap > 0 else 0
color_ent = "#10B981" if b_ent == "ABIERTA" else "#94A3B8"
color_sal = "#10B981" if b_sal == "ABIERTA" else "#94A3B8"

with c1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Plazas Disponibles</div>
        <div class="kpi-value">{libres} <span style="font-size:1rem;color:#64748B;">/ {cap}</span></div>
        <div class="kpi-foot">LCD: "{estado_db.get('lcd_linea_1')} | Libres: {libres}/{cap}"</div>
    </div>""", unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Ocupación Total</div>
        <div class="kpi-value">{ocupados} <span style="font-size:1rem;color:#3B82F6;">({pct_ocupacion}%)</span></div>
        <div class="kpi-foot">3 fijos + {int(esp1_ocupado) + int(esp2_ocupado)} en maqueta</div>
    </div>""", unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Barrera de Entrada</div>
        <div class="kpi-value" style="color:{color_ent};font-size:1.5rem;">{b_ent}</div>
        <div class="kpi-foot">LED Rojo: {'ENCENDIDO' if estado_db.get('led_rojo_entrada', True) else 'APAGADO'}</div>
    </div>""", unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">Barrera de Salida</div>
        <div class="kpi-value" style="color:{color_sal};font-size:1.5rem;">{b_sal}</div>
        <div class="kpi-foot">Sensor ultrasónico autónomo</div>
    </div>""", unsafe_allow_html=True)

st.write("")

# ====================
# NAVEGACIÓN PRINCIPAL (5 MÓDULOS DE PARKGATE AI)
# ====================
tab_in, tab_spaces, tab_placas, tab_ctrl, tab_hist = st.tabs([
    "📷 Escáner Entrada",
    "🅿️ Plazas & Auditoría Interna",
    "🪪 Padrón Vehicular",
    "🎛️ Centro de Control Admin",
    "📊 Bitácora & Alertas"
])

# ---------------------------------------------------------
# TAB 1: ESCÁNER DE ENTRADA (3 ESCENARIOS + ANTI-CLONACIÓN)
# ---------------------------------------------------------
with tab_in:
    col_cam, col_res = st.columns([1.1, 1], gap="large")

    with col_cam:
        with st.container(border=True):
            st.markdown("#### Cámara de Ingreso (Barrera Principal)")
            st.caption("Escanea la placa al llegar a la garita para validar registro y anti-clonación.")
            foto_entrada = st.camera_input("Cámara Entrada", label_visibility="collapsed", key="cam_entrada")

    with col_res:
        with st.container(border=True):
            st.markdown("#### Dictamen de Control de Acceso")
            if foto_entrada is not None:
                with st.spinner("Analizando placa con Gemini 2.5 Flash y verificando estado en MongoDB..."):
                    raw_texto, latencia = reconocer_placa_gemini(foto_entrada.getvalue(), foto_entrada.type)
                    placa = normalizar_placa(raw_texto)

                if not placa or "SINPLACA" in placa:
                    st.warning("No se detectó una placa legible. Enfoca mejor la matrícula e inténtalo nuevamente.")
                elif db is None:
                    st.error("Sin conexión a MongoDB Atlas.")
                else:
                    hora_lima = datetime.now(TZ_LIMA).strftime("%d/%m/%Y · %H:%M:%S")
                    registro = db["placas_autorizadas"].find_one({"placa": placa, "activo": True})

                    # CASO C: Placa NO registrada
                    if not registro:
                        enviar_comando_esp32("DENEGADO")
                        registrar_evento_db("ACCESO_DENEGADO", placa, "Placa no registrada en lista blanca", "ALERTA")
                        st.markdown(f"""
                        <div class="ticket-deny">
                            <div style="font-size:0.78rem;font-weight:700;color:#FB7185;letter-spacing:0.06em;">🚫 ACCESO DENEGADO · NO REGISTRADA ({latencia}s)</div>
                            <div class="plate-badge">{placa}</div>
                            <div style="color:#FECDD3;margin-top:0.35rem;">El vehículo no pertenece al estacionamiento privado.</div>
                            <div style="color:#94A3B8;font-size:0.82rem;margin-top:0.25rem;">Servo: <b>CERRADO</b> · LED Rojo: <b>ENCENDIDO</b> · LCD: <code>PLACA NO REGISTRADA</code> · {hora_lima}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    # CASO B: Placa registrada PERO el vehículo YA ESTÁ DENTRO (Posible clonación)
                    elif registro.get("dentro", False):
                        propietario = registro.get("propietario", "Titular")
                        espacio_asig = registro.get("espacio_asignado", "Sin asignar")
                        enviar_comando_esp32("ALERTA_DUPLICADA")
                        registrar_evento_db(
                            "ALERTA_CLONACION_PLACA",
                            placa,
                            f"Vehículo ya figura DENTRO ({espacio_asig}) - Titular: {propietario}",
                            "ALERTA"
                        )
                        st.markdown(f"""
                        <div class="ticket-warn">
                            <div style="font-size:0.78rem;font-weight:700;color:#FBBF24;letter-spacing:0.06em;">🚨 ALERTA DE SEGURIDAD · POSIBLE SUPLANTACIÓN ({latencia}s)</div>
                            <div class="plate-badge">{placa}</div>
                            <div style="color:#FEF3C7;margin-top:0.35rem;">El vehículo de <b>{propietario}</b> ({espacio_asig}) <b>YA SE ENCUENTRA DENTRO</b> del recinto.</div>
                            <div style="color:#FDE68A;font-size:0.83rem;margin-top:0.3rem;">Barrera bloqueada · LCD: <code>ALERTA SEGURIDAD / PLACA DUPLICADA</code> · {hora_lima}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    # CASO A: Placa registrada y vehículo FUERA del recinto -> Ingreso válido
                    else:
                        propietario = registro.get("propietario", "Titular")
                        espacio_asig = registro.get("espacio_asignado", "Espacio 1")
                        if libres <= 0:
                            st.error("Estacionamiento sin plazas libres (0/5).")
                        else:
                            db["placas_autorizadas"].update_one({"placa": placa}, {"$set": {"dentro": True}})
                            enviar_comando_esp32("ABRIR_ENTRADA")
                            registrar_evento_db("ACCESO_PERMITIDO", placa, f"Ingreso válido · {propietario} ({espacio_asig})", "INFO")
                            st.markdown(f"""
                            <div class="ticket-ok">
                                <div style="font-size:0.78rem;font-weight:700;color:#34D399;letter-spacing:0.06em;">✅ ACCESO PERMITIDO · INGRESO VÁLIDO ({latencia}s)</div>
                                <div class="plate-badge">{placa}</div>
                                <div style="color:#F8FAFC;margin-top:0.35rem;">Titular: <b>{propietario}</b> · Plaza asignada: <b>{espacio_asig}</b></div>
                                <div style="color:#A7F3D0;font-size:0.83rem;margin-top:0.3rem;">LED Rojo: <b>APAGADO</b> → Servo: <b>ABRIENDO</b> → Cierre automático · {hora_lima}</div>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.info("Esperando captura en garita de entrada. Se evaluarán los 3 escenarios automáticamente (Válido, Duplicado/Clonado o No Registrado).")

# ---------------------------------------------------------
# TAB 2: MONITOR INTERNO (ESPACIO 1 Y 2) + ESCÁNER AUDITORÍA
# ---------------------------------------------------------
with tab_spaces:
    st.markdown("#### Estado Físico de Plazas Sensorizadas (Maqueta)")
    sp1, sp2 = st.columns(2, gap="large")

    titular_e1 = db["placas_autorizadas"].find_one({"espacio_asignado": "Espacio 1", "activo": True}) if db is not None else None
    titular_e2 = db["placas_autorizadas"].find_one({"espacio_asignado": "Espacio 2", "activo": True}) if db is not None else None

    with sp1:
        clase1 = "space-card-busy" if esp1_ocupado else "space-card-free"
        estado_txt1 = "🔴 OCUPADO (Sensor detectó vehículo)" if esp1_ocupado else "🟢 LIBRE (LED Verde ENCENDIDO)"
        dueno1 = f"{titular_e1['placa']} · {titular_e1['propietario']}" if titular_e1 else "Sin vehículo vinculado"
        st.markdown(f"""
        <div class="{clase1}">
            <div style="font-size:0.8rem;font-weight:700;color:#94A3B8;">PLAZA PRIVADA 01</div>
            <div style="font-size:1.35rem;font-weight:700;color:#F8FAFC;margin:0.2rem 0;">Espacio 1</div>
            <div style="font-size:0.9rem;margin-bottom:0.4rem;">{estado_txt1}</div>
            <div style="font-size:0.82rem;color:#CBD5E1;">Titular asignado: <b>{dueno1}</b></div>
        </div>
        """, unsafe_allow_html=True)

    with sp2:
        clase2 = "space-card-busy" if esp2_ocupado else "space-card-free"
        estado_txt2 = "🔴 OCUPADO (Sensor detectó vehículo)" if esp2_ocupado else "🟢 LIBRE (LED Verde ENCENDIDO)"
        dueno2 = f"{titular_e2['placa']} · {titular_e2['propietario']}" if titular_e2 else "Sin vehículo vinculado"
        st.markdown(f"""
        <div class="{clase2}">
            <div style="font-size:0.8rem;font-weight:700;color:#94A3B8;">PLAZA PRIVADA 02</div>
            <div style="font-size:1.35rem;font-weight:700;color:#F8FAFC;margin:0.2rem 0;">Espacio 2</div>
            <div style="font-size:0.9rem;margin-bottom:0.4rem;">{estado_txt2}</div>
            <div style="font-size:0.82rem;color:#CBD5E1;">Titular asignado: <b>{dueno2}</b></div>
        </div>
        """, unsafe_allow_html=True)

    st.write("")
    with st.container(border=True):
        st.markdown("#### 🔍 Segundo Escáner LPR: Auditoría de Espacio Asignado")
        st.caption("Selecciona la plaza donde está estacionado el vehículo y toma una foto de su placa para verificar que respetó su lugar.")
        col_aud1, col_aud2 = st.columns([1.1, 1], gap="large")
        with col_aud1:
            espacio_evaluar = st.selectbox("¿En qué espacio está estacionado el vehículo?", ["Espacio 1", "Espacio 2"])
            foto_auditoria = st.camera_input("Cámara Auditoría Plaza", label_visibility="collapsed", key="cam_auditoria")

        with col_aud2:
            if foto_auditoria is not None:
                with st.spinner(f"Verificando titularidad en {espacio_evaluar}..."):
                    raw_aud, lat_aud = reconocer_placa_gemini(foto_auditoria.getvalue(), foto_auditoria.type)
                    placa_aud = normalizar_placa(raw_aud)

                if not placa_aud or "SINPLACA" in placa_aud:
                    st.warning("No se pudo leer la placa en el espacio. Acerca un poco más la cámara.")
                elif db is not None:
                    veh = db["placas_autorizadas"].find_one({"placa": placa_aud, "activo": True})
                    hora_lima = datetime.now(TZ_LIMA).strftime("%H:%M:%S")
                    if not veh:
                        registrar_evento_db("INVASION_NO_REGISTRADO", placa_aud, f"Vehículo no registrado ocupando {espacio_evaluar}", "ALERTA")
                        st.markdown(f"""
                        <div class="ticket-deny">
                            <div style="font-weight:700;color:#FB7185;">🚨 VEHÍCULO NO AUTORIZADO EN {espacio_evaluar.upper()}</div>
                            <div class="plate-badge">{placa_aud}</div>
                            <div style="color:#FECDD3;">Placa sin registro ocupando plaza privada ({hora_lima}).</div>
                        </div>""", unsafe_allow_html=True)
                    elif veh.get("espacio_asignado") == espacio_evaluar:
                        registrar_evento_db("ESTACIONAMIENTO_CORRECTO", placa_aud, f"Confirmado en su plaza ({espacio_evaluar})", "INFO")
                        st.markdown(f"""
                        <div class="ticket-ok">
                            <div style="font-weight:700;color:#34D399;">✅ PLAZA RESPETADA CORRECTAMENTE ({lat_aud}s)</div>
                            <div class="plate-badge">{placa_aud}</div>
                            <div style="color:#F8FAFC;">Titular <b>{veh.get('propietario')}</b> estacionado en su lugar asignado (<b>{espacio_evaluar}</b>).</div>
                        </div>""", unsafe_allow_html=True)
                    else:
                        espacio_real = veh.get("espacio_asignado", "Otra plaza")
                        enviar_comando_esp32("ALERTA_INVASION")
                        registrar_evento_db(
                            "ALERTA_INVASION_ESPACIO",
                            placa_aud,
                            f"Ocupó {espacio_evaluar} pero su plaza asignada es {espacio_real}",
                            "ALERTA"
                        )
                        st.markdown(f"""
                        <div class="ticket-warn">
                            <div style="font-weight:700;color:#FBBF24;">⚠️ ALERTA: INVASIÓN DE ESPACIO ASIGNADO ({lat_aud}s)</div>
                            <div class="plate-badge">{placa_aud}</div>
                            <div style="color:#FEF3C7;">El vehículo de <b>{veh.get('propietario')}</b> está en <b>{espacio_evaluar}</b>, pero su espacio asignado es <b>{espacio_real}</b>.</div>
                        </div>""", unsafe_allow_html=True)
            else:
                st.info("Toma una foto al vehículo estacionado para validar que ocupa su plaza asignada.")

# ---------------------------------------------------------
# TAB 3: PADRÓN VEHICULAR (ASIGNACIÓN DE ESPACIO 1 / 2)
# ---------------------------------------------------------
with tab_placas:
    c_form, c_list = st.columns([1, 1.5], gap="large")

    with c_form:
        with st.container(border=True):
            st.markdown("#### Registrar / Vincular Vehículo")
            st.caption("Asigna cada matrícula a su espacio privado en el estacionamiento.")
            with st.form("form_placa", clear_on_submit=True):
                nueva_placa = st.text_input("Matrícula", placeholder="Ej. BBW-266")
                propietario = st.text_input("Propietario", placeholder="Ej. Claudio Jara")
                espacio_sel = st.selectbox("Espacio Privado Asignado", ["Espacio 1", "Espacio 2", "Plaza 3 (Fija)", "Plaza 4 (Fija)", "Plaza 5 (Fija)"])
                guardado = st.form_submit_button("Guardar en Padrón", type="primary", use_container_width=True)
                if guardado and nueva_placa.strip() and db is not None:
                    p_norm = normalizar_placa(nueva_placa)
                    db["placas_autorizadas"].update_one(
                        {"placa": p_norm},
                        {"$set": {
                            "placa": p_norm,
                            "propietario": propietario.strip() or "Residente USIL",
                            "espacio_asignado": espacio_sel,
                            "dentro": False,
                            "activo": True
                        }},
                        upsert=True
                    )
                    st.success(f"Vehículo **{p_norm}** vinculado a **{espacio_sel}**.")
                    st.rerun()

    with c_list:
        with st.container(border=True):
            st.markdown("#### Vehículos Autorizados y Estado Actual")
            if db is not None:
                lista = list(db["placas_autorizadas"].find(
                    {"activo": True},
                    {"_id": 0, "placa": 1, "propietario": 1, "espacio_asignado": 1, "dentro": 1}
                ))
                if lista:
                    df_p = pd.DataFrame(lista)
                    df_p["Ubicación"] = df_p["dentro"].apply(lambda x: "DENTRO" if x else "FUERA")
                    df_p = df_p.drop(columns=["dentro"])
                    st.dataframe(df_p, use_container_width=True, hide_index=True)

                    c_act1, c_act2, c_act3 = st.columns([1.5, 1, 1])
                    with c_act1:
                        placa_sel = st.selectbox("Seleccionar placa", [x["placa"] for x in lista], label_visibility="collapsed")
                    with c_act2:
                        if st.button("Marcar FUERA", use_container_width=True):
                            db["placas_autorizadas"].update_one({"placa": placa_sel}, {"$set": {"dentro": False}})
                            st.rerun()
                    with c_act3:
                        if st.button("Revocar", use_container_width=True):
                            db["placas_autorizadas"].update_one({"placa": placa_sel}, {"$set": {"activo": False}})
                            st.rerun()
                else:
                    st.info("No hay vehículos registrados todavía.")

# ---------------------------------------------------------
# TAB 4: CENTRO DE CONTROL DEL ADMINISTRADOR (OVERRIDE HARDWARE)
# ---------------------------------------------------------
with tab_ctrl:
    st.markdown("#### Teleoperación Directa de Maqueta (Override Administrador)")
    col_s1, col_s2 = st.columns(2, gap="large")

    with col_s1:
        with st.container(border=True):
            st.markdown("##### 🚧 Servomotores de Barrera")
            b1, b2 = st.columns(2)
            with b1:
                if st.button("⬆️ Abrir Entrada", type="primary", use_container_width=True):
                    enviar_comando_esp32("ABRIR_ENTRADA")
                    registrar_evento_db("ADMIN_ABRIR_ENTRADA", "", "Override manual administrador", "INFO")
                    st.toast("Servo Entrada -> ABRIR", icon="⬆️")
                if st.button("⬇️ Cerrar Entrada", use_container_width=True):
                    enviar_comando_esp32("CERRAR_ENTRADA")
                    st.toast("Servo Entrada -> CERRAR", icon="⬇️")
            with b2:
                if st.button("⬆️ Abrir Salida", type="primary", use_container_width=True):
                    enviar_comando_esp32("ABRIR_SALIDA")
                    registrar_evento_db("ADMIN_ABRIR_SALIDA", "", "Override manual administrador", "INFO")
                    st.toast("Servo Salida -> ABRIR", icon="⬆️")
                if st.button("⬇️ Cerrar Salida", use_container_width=True):
                    enviar_comando_esp32("CERRAR_SALIDA")
                    st.toast("Servo Salida -> CERRAR", icon="⬇️")

        with st.container(border=True):
            st.markdown("##### 📟 Pantalla LCD 16x2")
            l1 = st.text_input("Línea 1 (máx 16 caracteres)", value="SMART PARKING USIL", max_chars=16)
            l2 = st.text_input("Línea 2 (máx 16 caracteres)", value=f"Libres: {libres} / {cap}", max_chars=16)
            cl1, cl2 = st.columns(2)
            with cl1:
                if st.button("Enviar Texto a LCD", type="primary", use_container_width=True):
                    payload_lcd = json.dumps({"l1": l1, "l2": l2})
                    enviar_comando_esp32(payload_lcd, topic=TOPIC_LCD)
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"lcd_linea_1": l1, "lcd_linea_2": l2}})
                    st.toast("Mensaje actualizado en LCD", icon="📟")
            with cl2:
                if st.button("Restaurar LCD Base", use_container_width=True):
                    enviar_comando_esp32("LCD_DEFAULT")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"lcd_linea_1": "SMART PARKING USIL", "lcd_linea_2": f"Libres: {libres} / {cap}"}})
                    st.rerun()

    with col_s2:
        with st.container(border=True):
            st.markdown("##### 💡 Control Manual de Indicadores LED")
            st.caption("Enciende o apaga individualmente los LEDs rojos de barrera y los LEDs verdes de plaza.")
            ld1, ld2 = st.columns(2)
            with ld1:
                if st.button("🔴 LED Rojo Entrada ON", use_container_width=True):
                    enviar_comando_esp32("LED_ROJO_IN_ON")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_rojo_entrada": True}})
                    st.rerun()
                if st.button("⚫ LED Rojo Entrada OFF", use_container_width=True):
                    enviar_comando_esp32("LED_ROJO_IN_OFF")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_rojo_entrada": False}})
                    st.rerun()
                if st.button("🟢 LED Verde Espacio 1 ON", use_container_width=True):
                    enviar_comando_esp32("LED_VERDE_1_ON")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_verde_1": True, "espacio_1_ocupado": False}})
                    st.rerun()
                if st.button("⚫ LED Verde Espacio 1 OFF", use_container_width=True):
                    enviar_comando_esp32("LED_VERDE_1_OFF")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_verde_1": False, "espacio_1_ocupado": True}})
                    st.rerun()

            with ld2:
                if st.button("🔴 LED Rojo Salida ON", use_container_width=True):
                    enviar_comando_esp32("LED_ROJO_OUT_ON")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_rojo_salida": True}})
                    st.rerun()
                if st.button("⚫ LED Rojo Salida OFF", use_container_width=True):
                    enviar_comando_esp32("LED_ROJO_OUT_OFF")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_rojo_salida": False}})
                    st.rerun()
                if st.button("🟢 LED Verde Espacio 2 ON", use_container_width=True):
                    enviar_comando_esp32("LED_VERDE_2_ON")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_verde_2": True, "espacio_2_ocupado": False}})
                    st.rerun()
                if st.button("⚫ LED Verde Espacio 2 OFF", use_container_width=True):
                    enviar_comando_esp32("LED_VERDE_2_OFF")
                    if db is not None:
                        db["estado_sistema"].update_one({"_id": "p01"}, {"$set": {"led_verde_2": False, "espacio_2_ocupado": True}})
                    st.rerun()

        with st.container(border=True):
            st.markdown("##### 🔄 Sincronización y Reset de Maqueta")
            r1, r2 = st.columns(2)
            with r1:
                if st.button("Refrescar Telemetría", use_container_width=True):
                    st.rerun()
            with r2:
                if st.button("Resetear Estado Base (2/5)", type="primary", use_container_width=True):
                    enviar_comando_esp32("RESET_SISTEMA")
                    if db is not None:
                        db["placas_autorizadas"].update_many({}, {"$set": {"dentro": False}})
                        db["estado_sistema"].update_one(
                            {"_id": "p01"},
                            {"$set": {
                                "ocupados_base": 3, "espacio_1_ocupado": False, "espacio_2_ocupado": False,
                                "led_verde_1": True, "led_verde_2": True,
                                "led_rojo_entrada": True, "led_rojo_salida": True,
                                "lcd_linea_1": "SMART PARKING USIL", "lcd_linea_2": "Libres: 2 / 5"
                            }},
                            upsert=True
                        )
                    st.toast("Sistema reiniciado a estado base (2/5 libres)", icon="🔄")
                    st.rerun()

# ---------------------------------------------------------
# TAB 5: BITÁCORA Y AUDITORÍA EN MONGODB ATLAS
# ---------------------------------------------------------
with tab_hist:
    with st.container(border=True):
        h1, h2 = st.columns([3.5, 1])
        with h1:
            st.markdown("#### Auditoría Centralizada de Accesos e Incidentes")
            st.caption("Historial persistido en MongoDB Atlas (Hora Perú UTC-5).")
        with h2:
            if st.button("🔄 Actualizar Bitácora", use_container_width=True):
                st.rerun()

        if db is not None:
            eventos = list(db["eventos_parking"].find({}, {"_id": 0}).sort("fecha_utc", -1).limit(50))
            if eventos:
                df_ev = pd.DataFrame(eventos)
                if "fecha_utc" in df_ev.columns:
                    df_ev["Hora Lima"] = pd.to_datetime(df_ev["fecha_utc"], utc=True).dt.tz_convert(TZ_LIMA).dt.strftime("%d/%m/%Y %H:%M:%S")
                    df_ev = df_ev.drop(columns=["fecha_utc"])
                st.dataframe(df_ev, use_container_width=True, hide_index=True)
            else:
                st.info("No hay eventos registrados todavía.")