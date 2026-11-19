import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
import pandas as pd
import streamlit as st
import pymongo
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from google import genai
from google.genai import types

# ====================
# CONFIGURACIÓN
# ====================
st.set_page_config(
    page_title="ParkGate · Control de acceso",
    page_icon=":material/local_parking:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

MONGODB_URI    = os.getenv("MONGODB_URI") or st.secrets.get("app", {}).get("MONGODB_URI", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or st.secrets.get("app", {}).get("GOOGLE_API_KEY", "")

BROKER       = "broker.hivemq.com"
PORT         = 1883
TOPIC_ESTADO = "usil/iot/parking/p01/estado"
TOPIC_DISPON = "usil/iot/parking/p01/disponibilidad"
TOPIC_CMD    = "usil/iot/parking/p01/cmd"
TZ_LIMA      = timezone(timedelta(hours=-5))

# ====================
# ICONOS (SVG en línea, estilo Lucide)
# ====================
ICONS = {
    "shield":  '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/><path d="m9 12 2 2 4-4"/>',
    "check":   '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>',
    "ban":     '<circle cx="12" cy="12" r="10"/><path d="m4.9 4.9 14.2 14.2"/>',
    "alert":   '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
    "parking": '<rect width="18" height="18" x="3" y="3" rx="2"/><path d="M9 17V7h4a3 3 0 0 1 0 6H9"/>',
    "login":   '<path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><path d="m10 17 5-5-5-5"/><path d="M15 12H3"/>',
    "logout":  '<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><path d="m16 17 5-5-5-5"/><path d="M21 12H9"/>',
    "wifi":    '<path d="M12 20h.01"/><path d="M2 8.82a15 15 0 0 1 20 0"/><path d="M5 12.859a10 10 0 0 1 14 0"/><path d="M8.5 16.429a5 5 0 0 1 7 0"/>',
    "wifioff": '<path d="M12 20h.01"/><path d="M8.5 16.429a5 5 0 0 1 7 0"/><path d="M5 12.859a10 10 0 0 1 5.17-2.69"/><path d="M19 12.859a10 10 0 0 0-2.007-1.523"/><path d="M2 8.82a15 15 0 0 1 4.177-2.643"/><path d="M22 8.82a15 15 0 0 0-11.288-3.764"/><path d="m2 2 20 20"/>',
}

def ico(name, size=18, sw=2):
    return (f'<svg class="ico" width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
            f'stroke="currentColor" stroke-width="{sw}" stroke-linecap="round" stroke-linejoin="round">'
            f'{ICONS[name]}</svg>')

# ====================
# ESTILOS
# ====================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700&display=swap');
:root {
  --bg:#0E1116; --panel:#151A21; --line:#252C36; --text:#E8ECF1; --muted:#8A94A3;
  --ok:#3FB68B; --deny:#E5484D; --warn:#E0A526; --blue:#4C8DFF;
}
#MainMenu, footer {visibility:hidden;}
.block-container {padding-top:1.4rem; padding-bottom:2.5rem; max-width:1180px;}
.ico {vertical-align:-3px; flex-shrink:0;}

/* Encabezado */
.pg-head {display:flex; flex-wrap:wrap; justify-content:space-between; align-items:center;
  gap:1rem; padding-bottom:1rem; margin-bottom:1.1rem; border-bottom:1px solid var(--line);}
.pg-brand {display:flex; align-items:center; gap:.75rem;}
.pg-brand .ico {color:var(--blue);}
.pg-title {font-size:1.35rem; font-weight:650; color:var(--text); line-height:1.2; margin:0;}
.pg-sub {font-size:.85rem; color:var(--muted); margin:.1rem 0 0;}
.pg-pill {display:inline-flex; align-items:center; gap:.5rem; padding:.4rem .85rem;
  border-radius:999px; font-size:.84rem; font-weight:600; border:1px solid;}
.pg-pill.on  {color:var(--ok);   border-color:rgba(63,182,139,.45); background:rgba(63,182,139,.10);}
.pg-pill.off {color:var(--deny); border-color:rgba(229,72,77,.45);  background:rgba(229,72,77,.10);}

/* Panel de estado */
.pg-panel {background:var(--panel); border:1px solid var(--line); border-radius:10px;
  padding:1.1rem 1.3rem; height:100%;}
.pg-label {display:flex; align-items:center; gap:.45rem; font-size:.88rem; color:var(--muted); font-weight:500;}
.pg-big {font-size:2.5rem; font-weight:700; color:var(--text); line-height:1.1;
  font-variant-numeric:tabular-nums; margin:.35rem 0 .15rem;}
.pg-big small {font-size:1.05rem; color:var(--muted); font-weight:500;}
.pg-foot {font-size:.82rem; color:var(--muted);}
.pg-slots {display:flex; gap:6px; margin-top:.85rem;}
.pg-slot {flex:1; max-width:56px; height:26px; border-radius:4px; border:2px solid var(--line);}
.pg-slot.busy {background:var(--blue); border-color:var(--blue);}
.pg-state {font-size:1.5rem; font-weight:650; margin:.5rem 0 .1rem;}

/* Resultado del escáner */
.pg-verdict {border:1px solid var(--line); border-left:5px solid var(--c); border-radius:10px;
  background:var(--panel); padding:1.1rem 1.3rem; margin-top:.5rem;}
.pg-verdict-title {display:flex; align-items:center; gap:.55rem; font-size:1.05rem;
  font-weight:650; color:var(--c);}
.pg-detail {color:var(--text); font-size:.95rem; margin-top:.5rem;}
.pg-meta {color:var(--muted); font-size:.82rem; margin-top:.25rem;}

/* Placa */
.pg-plate {display:inline-flex; flex-direction:column; align-items:center; background:#F7F7F2;
  border:2px solid #1B1F26; border-radius:6px; padding:.1rem 1.1rem .2rem; margin:.7rem 0 .1rem;
  box-shadow:0 2px 8px rgba(0,0,0,.45);}
.pg-plate-top {font-family:'Barlow Condensed','Arial Narrow',sans-serif; font-weight:600;
  font-size:.68rem; letter-spacing:.3em; color:#B3262B; margin-bottom:-.15rem;}
.pg-plate-num {font-family:'Barlow Condensed','Arial Narrow',sans-serif; font-weight:700;
  font-size:2.6rem; letter-spacing:.08em; color:#12151A; line-height:1.1;}

/* Pestañas */
.stTabs [data-baseweb="tab-list"] {gap:4px; border-bottom:1px solid var(--line);}
.stTabs [data-baseweb="tab"] {padding:.55rem 1rem; font-weight:600;}
.stButton > button, .stFormSubmitButton > button {border-radius:8px; font-weight:600;}
</style>
""", unsafe_allow_html=True)

# ====================
# CONEXIONES
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
        st.error(f"No se pudo enviar el comando {cmd}: {e}")
        return False

def normalizar_placa(texto: str) -> str:
    limpio = re.sub(r"[^A-Z0-9]", "", texto.upper())
    return f"{limpio[:3]}-{limpio[3:]}" if len(limpio) == 6 else limpio

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
        contents=[types.Part.from_bytes(data=imagen_bytes, mime_type=mime_type), prompt],
    )
    return response.text.strip()

def registrar_evento_db(evento: str, placa: str = "", detalle: str = ""):
    if db is not None:
        db["eventos_parking"].insert_one({
            "evento": evento, "placa": placa, "detalle": detalle,
            "origen": "WEB_LPR", "fecha_utc": datetime.now(timezone.utc),
        })

def evaluar_placa(foto, libres):
    """Reconoce la placa, decide el acceso y ejecuta los efectos (MQTT y log) una sola vez."""
    hora = datetime.now(TZ_LIMA).strftime("%d/%m/%Y %H:%M:%S")
    try:
        raw = reconocer_placa_gemini(foto.getvalue(), foto.type)
    except Exception as e:
        return {"tipo": "error", "placa": "", "hora": hora, "detalle": str(e)}
    if raw == "SIN_API_KEY":
        return {"tipo": "config", "placa": "", "hora": hora, "detalle": "Falta GOOGLE_API_KEY."}
    placa = normalizar_placa(raw)
    if not placa or "SINPLACA" in placa:
        return {"tipo": "sin_placa", "placa": "", "hora": hora, "detalle": ""}
    if db is None:
        return {"tipo": "config", "placa": placa, "hora": hora, "detalle": "Falta configurar MONGODB_URI."}

    registro = db["placas_autorizadas"].find_one({"placa": placa, "activo": True})
    if not registro:
        registrar_evento_db("ACCESO_DENEGADO", placa, "Placa no autorizada")
        return {"tipo": "denegado", "placa": placa, "hora": hora, "detalle": ""}

    titular = registro.get("propietario", "Residente autorizado")
    if libres <= 0:
        registrar_evento_db("ACCESO_DENEGADO_LLENO", placa, titular)
        return {"tipo": "lleno", "placa": placa, "hora": hora, "detalle": titular}

    enviar_comando_esp32("ABRIR_ENTRADA")
    registrar_evento_db("ACCESO_PLACA_OK", placa, f"Autorizado: {titular}")
    return {"tipo": "ok", "placa": placa, "hora": hora, "detalle": titular}

def veredicto_html(v):
    placa_html = (f'<div class="pg-plate"><span class="pg-plate-top">PERÚ</span>'
                  f'<span class="pg-plate-num">{v["placa"]}</span></div>') if v["placa"] else ""
    cfg = {
        "ok":      ("var(--ok)",   "check",  "Acceso autorizado",
                    f'Titular: <b>{v["detalle"]}</b>', "Orden de apertura enviada a la barrera de entrada."),
        "lleno":   ("var(--warn)", "alert",  "Estacionamiento completo",
                    f'Titular: <b>{v["detalle"]}</b>', "No hay plazas libres. La barrera permanece cerrada."),
        "denegado":("var(--deny)", "ban",    "Acceso denegado",
                    "La placa no está en la lista de vehículos autorizados.", "Incidente registrado en la bitácora."),
        "error":   ("var(--deny)", "alert",  "No se pudo leer la placa",
                    "Falló el reconocimiento de imagen. Intenta de nuevo.", v["detalle"]),
        "config":  ("var(--warn)", "alert",  "Configuración incompleta", v["detalle"], ""),
        "sin_placa":("var(--warn)","alert",  "No se detectó una placa legible",
                    "Acerca la cámara a la placa y vuelve a tomar la foto.", ""),
    }[v["tipo"]]
    color, icono, titulo, detalle, meta = cfg
    meta_txt = f"{meta} {v['hora']}".strip()
    return (f'<div class="pg-verdict" style="--c:{color}">'
            f'<div class="pg-verdict-title">{ico(icono, 22)}{titulo}</div>{placa_html}'
            f'<div class="pg-detail">{detalle}</div><div class="pg-meta">{meta_txt}</div></div>')

# ====================
# TELEMETRÍA Y ENCABEZADO
# ====================
mqtt_data = obtener_estado_mqtt()
est  = mqtt_data["estado"] or {}
disp = mqtt_data["disponibilidad"]

libres   = int(est.get("libres", 5))
ocupados = int(est.get("ocupados", 0))
cap      = int(est.get("cap", 5))
b_ent    = est.get("barrera_entrada", "CERRADA")
b_sal    = est.get("barrera_salida", "CERRADA")
online   = (disp == "online")

pill = (f'<span class="pg-pill on">{ico("wifi", 16)}Nodo ESP32 en línea</span>' if online
        else f'<span class="pg-pill off">{ico("wifioff", 16)}Nodo ESP32 sin conexión</span>')

st.markdown(
    f'<div class="pg-head"><div class="pg-brand">{ico("shield", 30, 1.8)}'
    f'<div><div class="pg-title">ParkGate</div>'
    f'<div class="pg-sub">Control de acceso vehicular con lectura automática de placas</div></div></div>'
    f'<div>{pill}</div></div>', unsafe_allow_html=True)

pct = int(ocupados / cap * 100) if cap > 0 else 0
slots = "".join(f'<span class="pg-slot{" busy" if i < ocupados else ""}"></span>' for i in range(cap))

def tile_barrera(titulo, icono, estado):
    abierta = (estado == "ABIERTA")
    color = "var(--ok)" if abierta else "var(--muted)"
    return (f'<div class="pg-panel"><div class="pg-label">{ico(icono, 17)}{titulo}</div>'
            f'<div class="pg-state" style="color:{color}">{estado.capitalize()}</div>'
            f'<div class="pg-foot">{"Vehículo pasando" if abierta else "Cerrada"}</div></div>')

k1, k2, k3 = st.columns([1.7, 1, 1], gap="medium")
with k1:
    st.markdown(
        f'<div class="pg-panel"><div class="pg-label">{ico("parking", 17)}Plazas libres</div>'
        f'<div class="pg-big">{libres} <small>de {cap}</small></div>'
        f'<div class="pg-foot">{ocupados} ocupadas ({pct}%)</div>'
        f'<div class="pg-slots">{slots}</div></div>', unsafe_allow_html=True)
with k2:
    st.markdown(tile_barrera("Barrera de entrada", "login", b_ent), unsafe_allow_html=True)
with k3:
    st.markdown(tile_barrera("Barrera de salida", "logout", b_sal), unsafe_allow_html=True)

st.write("")

# ====================
# NAVEGACIÓN
# ====================
tab_cam, tab_placas, tab_ctrl, tab_hist = st.tabs([
    ":material/photo_camera: Escáner de placas",
    ":material/badge: Vehículos autorizados",
    ":material/garage: Barreras",
    ":material/history: Bitácora",
])

# ---------------------------------------------------------
# TAB 1: ESCÁNER LPR
# ---------------------------------------------------------
with tab_cam:
    col_izq, col_der = st.columns([1.15, 1], gap="large")

    with col_izq:
        with st.container(border=True):
            st.markdown("#### Captura de placa")
            st.caption("Enfoca la placa delantera o trasera con buena iluminación.")
            foto = st.camera_input("Cámara de control de acceso", label_visibility="collapsed")

    with col_der:
        with st.container(border=True):
            st.markdown("#### Resultado")
            if foto is not None:
                # Evita reenviar la orden de apertura y duplicar logs en cada rerun
                huella = hashlib.md5(foto.getvalue()).hexdigest()
                if st.session_state.get("foto_huella") != huella:
                    with st.spinner("Leyendo la placa..."):
                        st.session_state["veredicto"] = evaluar_placa(foto, libres)
                    st.session_state["foto_huella"] = huella
                st.markdown(veredicto_html(st.session_state["veredicto"]), unsafe_allow_html=True)
            else:
                st.session_state.pop("foto_huella", None)
                st.info("Toma una foto de la placa. Se validará contra la lista de vehículos autorizados.",
                        icon=":material/photo_camera:")

# ---------------------------------------------------------
# TAB 2: VEHÍCULOS AUTORIZADOS
# ---------------------------------------------------------
with tab_placas:
    c_form, c_list = st.columns([1, 1.45], gap="large")

    with c_form:
        with st.container(border=True):
            st.markdown("#### Registrar vehículo")
            st.caption("Agrega o actualiza placas con apertura automática.")
            with st.form("form_placa", clear_on_submit=True):
                nueva_placa = st.text_input("Placa", placeholder="Ej. BBW-266")
                propietario = st.text_input("Propietario o unidad", placeholder="Ej. Claudio Jara, Dpto 402")
                guardado = st.form_submit_button("Autorizar placa", type="primary",
                                                 icon=":material/add:", use_container_width=True)
                if guardado and nueva_placa.strip() and db is not None:
                    p_norm = normalizar_placa(nueva_placa)
                    db["placas_autorizadas"].update_one(
                        {"placa": p_norm},
                        {"$set": {"placa": p_norm,
                                  "propietario": propietario.strip() or "Sin especificar",
                                  "activo": True,
                                  "actualizado_utc": datetime.now(timezone.utc)}},
                        upsert=True,
                    )
                    st.toast(f"Placa {p_norm} autorizada", icon=":material/check_circle:")
                    st.rerun()

    with c_list:
        with st.container(border=True):
            st.markdown("#### Vehículos autorizados")
            if db is not None:
                lista = list(db["placas_autorizadas"].find({"activo": True}, {"_id": 0, "placa": 1, "propietario": 1}))
                if lista:
                    df_p = pd.DataFrame(lista).rename(columns={"placa": "Placa", "propietario": "Propietario"})
                    st.dataframe(df_p, use_container_width=True, hide_index=True)
                    d1, d2 = st.columns([2, 1])
                    with d1:
                        placa_borrar = st.selectbox("Revocar acceso", [x["placa"] for x in lista],
                                                    label_visibility="collapsed")
                    with d2:
                        if st.button("Revocar", icon=":material/block:", use_container_width=True):
                            db["placas_autorizadas"].update_one({"placa": placa_borrar}, {"$set": {"activo": False}})
                            st.rerun()
                else:
                    st.info("Aún no hay vehículos autorizados. Registra el primero con el formulario.",
                            icon=":material/badge:")
            else:
                st.error("Falta configurar la conexión a MongoDB Atlas.", icon=":material/database_off:")

# ---------------------------------------------------------
# TAB 3: BARRERAS
# ---------------------------------------------------------
with tab_ctrl:
    col_m1, col_m2 = st.columns(2, gap="large")

    with col_m1:
        with st.container(border=True):
            st.markdown("#### Barrera de entrada")
            st.caption("Acciona el servomotor de ingreso.")
            b1, b2 = st.columns(2)
            with b1:
                if st.button("Abrir", key="ab_ent", type="primary", icon=":material/lock_open:",
                             use_container_width=True):
                    if enviar_comando_esp32("ABRIR_ENTRADA"):
                        registrar_evento_db("APERTURA_MANUAL_ENTRADA", "", "Consola operador")
                        st.toast("Entrada: orden de apertura enviada", icon=":material/check_circle:")
            with b2:
                if st.button("Cerrar", key="ce_ent", icon=":material/lock:", use_container_width=True):
                    if enviar_comando_esp32("CERRAR_ENTRADA"):
                        st.toast("Entrada: orden de cierre enviada", icon=":material/lock:")

    with col_m2:
        with st.container(border=True):
            st.markdown("#### Barrera de salida")
            st.caption("Acciona el servomotor de salida.")
            b3, b4 = st.columns(2)
            with b3:
                if st.button("Abrir", key="ab_sal", type="primary", icon=":material/lock_open:",
                             use_container_width=True):
                    if enviar_comando_esp32("ABRIR_SALIDA"):
                        registrar_evento_db("APERTURA_MANUAL_SALIDA", "", "Consola operador")
                        st.toast("Salida: orden de apertura enviada", icon=":material/check_circle:")
            with b4:
                if st.button("Cerrar", key="ce_sal", icon=":material/lock:", use_container_width=True):
                    if enviar_comando_esp32("CERRAR_SALIDA"):
                        st.toast("Salida: orden de cierre enviada", icon=":material/lock:")

    st.write("")
    if st.button("Actualizar estado", icon=":material/sync:", use_container_width=True):
        st.rerun()

# ---------------------------------------------------------
# TAB 4: BITÁCORA
# ---------------------------------------------------------
with tab_hist:
    with st.container(border=True):
        h1, h2 = st.columns([3, 1])
        with h1:
            st.markdown("#### Bitácora de accesos e incidentes")
            st.caption("Últimos 50 eventos guardados en MongoDB Atlas. Hora de Lima (UTC-5).")
        with h2:
            if st.button("Actualizar", icon=":material/sync:", use_container_width=True):
                st.rerun()

        if db is not None:
            eventos = list(db["eventos_parking"].find({}, {"_id": 0}).sort("fecha_utc", -1).limit(50))
            if eventos:
                df = pd.DataFrame(eventos)
                if "fecha_utc" in df.columns:
                    df["Hora"] = (pd.to_datetime(df["fecha_utc"], utc=True)
                                  .dt.tz_convert(TZ_LIMA).dt.strftime("%d/%m/%Y %H:%M:%S"))
                    df = df.drop(columns=["fecha_utc"])
                df = df.rename(columns={"evento": "Evento", "placa": "Placa",
                                        "detalle": "Detalle", "origen": "Origen"})
                orden = [c for c in ["Hora", "Evento", "Placa", "Detalle", "Origen"] if c in df.columns]
                st.dataframe(df[orden], use_container_width=True, hide_index=True)
            else:
                st.info("Todavía no hay eventos. Aparecerán aquí al escanear una placa o accionar una barrera.",
                        icon=":material/history:")
