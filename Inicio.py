import pandas as pd
import streamlit as st
import numpy as np
from datetime import datetime
import time
import plotly.graph_objects as go
import plotly.express as px
from influxdb_client import InfluxDBClient

# ── Configuración de página ────────────────────────────────────────────
st.set_page_config(
    page_title="Monitor de Pipeta de Gas",
    page_icon="🔥",
    layout="wide"
)

st.markdown("""
    <style>
    .metric-card {
        background: #1e1e1e;
        border-radius: 12px;
        padding: 1.2rem;
        text-align: center;
        border: 1px solid #333;
    }
    .alerta-roja    { color: #E74C3C; font-size: 1.4rem; font-weight: bold; }
    .alerta-amarilla{ color: #F39C12; font-size: 1.4rem; font-weight: bold; }
    .alerta-verde   { color: #2ECC71; font-size: 1.4rem; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# ── Constantes ─────────────────────────────────────────────────────────
TOKEN  = 'QXCo7Iznp0LkBZ_lt3y04mLIs1hElSgcuTqWrToLwF9YtayXiu4FhbjxuFALKPOj89ZEKSXP1jifWmrl6LPauA=='
ORG    = 'organinizaciondaniel'
BUCKET = 'gasBUCKET'
URL    = 'https://us-east-1-1.aws.cloud2.influxdata.com/'

UMBRAL_CRITICO = 1000
UMBRAL_MEDIO   = 2500
UMBRAL_LLENO   = 4000

# ── Funciones ──────────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    return InfluxDBClient(url=URL, token=TOKEN, org=ORG, verify_ssl=False)

def consultar_gas(horas=24):
    client = get_client()
    query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -{horas}h)
      |> filter(fn: (r) => r._field == "gas")
    '''
    tablas = client.query_api().query(query, org=ORG)
    tiempos, valores = [], []
    for tabla in tablas:
        for record in tabla.records:
            tiempos.append(record.get_time())
            valores.append(record.get_value())

    idx = pd.DatetimeIndex(
        pd.to_datetime(pd.Series(tiempos), utc=True)
    ).tz_convert('America/Bogota')
    serie = pd.Series(valores, index=idx, name='gas', dtype=float)
    return serie.sort_index()

def nivel_pipeta(valor):
    pct = min(100, max(0, (valor / UMBRAL_LLENO) * 100))
    if pct > 60:
        return pct, '🟢', 'Llena', '#2ECC71'
    elif pct > 25:
        return pct, '🟡', 'Media', '#F39C12'
    else:
        return pct, '🔴', 'Crítica', '#E74C3C'

def estimar_dias(serie):
    if len(serie) < 2:
        return None
    consumo_hora = (serie.iloc[0] - serie.iloc[-1]) / max(1, len(serie) / 60)
    if consumo_hora <= 0:
        return None
    ultimo = serie.iloc[-1]
    horas_restantes = (ultimo - UMBRAL_CRITICO) / consumo_hora
    return max(0, horas_restantes / 24)

# ══════════════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════════════
st.title('🔥 Monitor de Pipeta de Gas')
st.caption('Universidad EAFIT · Sensor ESP32/MQ · InfluxDB Cloud')

# ── Sidebar ────────────────────────────────────────────────────────────
with st.sidebar:
    st.header('⚙️ Configuración')
    horas      = st.slider('Ventana de tiempo (horas)', 1, 48, 24)
    auto_ref   = st.toggle('Auto-refresh (30s)', value=False)
    umbral_usr = st.number_input('Umbral alerta crítica (PPM)', 0, 5000, UMBRAL_CRITICO)
    st.divider()
    st.caption('Fuente: InfluxDB Cloud')
    st.caption(f'Bucket: `{BUCKET}`')

UMBRAL_CRITICO = umbral_usr

# ── Cargar datos ───────────────────────────────────────────────────────
serie = None

with st.spinner('Cargando datos desde InfluxDB...'):
    try:
        serie = consultar_gas(horas)
    except Exception as e:
        st.warning(f'No se pudo conectar a InfluxDB: {e}')

if serie is None:
    st.info('Carga un CSV como alternativa:')
    f = st.file_uploader('CSV con columnas Time y gas', type=['csv'])
    if f:
        df_raw = pd.read_csv(f)
        # Limpiar nombres de columnas
        df_raw.columns = df_raw.columns.str.strip()

        # Detectar columna de tiempo
        time_col = next((c for c in df_raw.columns
                         if c.lower() in ['time', 'fecha', 'timestamp', 'datetime']), None)
        if time_col:
            df_raw[time_col] = pd.to_datetime(df_raw[time_col])
            df_raw = df_raw.set_index(time_col)
        else:
            df_raw.index = pd.RangeIndex(len(df_raw))

        # Detectar columna de gas
        col_gas = next((c for c in df_raw.columns
                        if 'gas' in c.lower()), None)
        if col_gas is None:
            col_gas = df_raw.columns[0]

        st.info(f'Columna detectada: `{col_gas}`')
        serie = df_raw[col_gas].dropna().astype(float)
        serie.name = 'gas'
    else:
        st.stop()

# ── Valores clave ──────────────────────────────────────────────────────
ultimo  = float(serie.iloc[-1])
pct, emoji, estado, color = nivel_pipeta(ultimo)
dias    = estimar_dias(serie)

# ══════════════════════════════════════════════════════════════════════
#  FILA 1 — KPIs
# ══════════════════════════════════════════════════════════════════════
st.subheader('📊 Estado Actual')
c1, c2, c3, c4 = st.columns(4)

c1.metric('Lectura actual',  f'{ultimo:.0f} PPM',
          delta=f'{ultimo - float(serie.mean()):.0f} vs promedio')
c2.metric('Estado pipeta',   f'{emoji} {estado}')
c3.metric('Nivel estimado',  f'{pct:.1f} %')
c4.metric('Días restantes',  f'{dias:.1f} días' if dias else 'Sin datos')

# ── Alerta ─────────────────────────────────────────────────────────────
if ultimo < UMBRAL_CRITICO:
    st.error('🚨 ¡ALERTA! Nivel de gas crítico — considera reemplazar la pipeta pronto.')
elif pct < 60:
    st.warning('⚠️ Nivel medio — monitorea con frecuencia.')
else:
    st.success('✅ Pipeta en buen estado.')

# ══════════════════════════════════════════════════════════════════════
#  FILA 2 — Gauge + Serie de tiempo
# ══════════════════════════════════════════════════════════════════════
col_gauge, col_serie = st.columns([1, 2])

with col_gauge:
    st.subheader('🔋 Nivel visual')
    fig_gauge = go.Figure(go.Indicator(
        mode   = 'gauge+number+delta',
        value  = pct,
        delta  = {'reference': 50},
        title  = {'text': 'Nivel de Gas (%)'},
        gauge  = {
            'axis'  : {'range': [0, 100]},
            'bar'   : {'color': color},
            'steps' : [
                {'range': [0,  25], 'color': '#fadbd8'},
                {'range': [25, 60], 'color': '#fdebd0'},
                {'range': [60, 100],'color': '#d5f5e3'},
            ],
            'threshold': {
                'line' : {'color': 'red', 'width': 4},
                'thickness': 0.75,
                'value': 25
            }
        }
    ))
    fig_gauge.update_layout(height=300, margin=dict(t=40, b=0))
    st.plotly_chart(fig_gauge, use_container_width=True)

with col_serie:
    st.subheader('📈 Serie de tiempo')
    fig_ts = go.Figure()
    fig_ts.add_trace(go.Scatter(
        x=serie.index, y=serie.values,
        fill='tozeroy', fillcolor='rgba(231,76,60,0.15)',
        line=dict(color='#E74C3C', width=2),
        name='Gas (PPM)'
    ))
    fig_ts.add_hline(y=UMBRAL_CRITICO, line_dash='dash',
                     line_color='red',    annotation_text='Crítico')
    fig_ts.add_hline(y=UMBRAL_MEDIO,   line_dash='dash',
                     line_color='orange', annotation_text='Medio')
    fig_ts.update_layout(
        height=300,
        xaxis_title='Hora (Colombia)',
        yaxis_title='Gas (PPM)',
        margin=dict(t=10, b=40)
    )
    st.plotly_chart(fig_ts, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════
#  TABS
# ══════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs([
    '📉 Consumo', '📊 Estadísticas', '🔍 Filtros', '📍 Ubicación'
])

# ── Tab 1: Consumo ─────────────────────────────────────────────────────
with tab1:
    st.subheader('Historial de consumo')

    if isinstance(serie.index, pd.DatetimeIndex):
        franjas = serie.resample('30min').mean().dropna()
        labels  = franjas.index.strftime('%H:%M')
    else:
        franjas = serie
        labels  = franjas.index.astype(str)

    fig_bar = px.bar(
        x=labels, y=franjas.values,
        labels={'x': 'Franja horaria', 'y': 'Gas promedio (PPM)'},
        color=franjas.values,
        color_continuous_scale=['#2ECC71', '#F39C12', '#E74C3C'][::-1],
        title='Consumo promedio por franja de 30 min'
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader('Tasa de cambio (PPM/min)')
    tasa = serie.diff()
    fig_tasa = go.Figure()
    fig_tasa.add_trace(go.Scatter(
        x=tasa.index, y=tasa.values,
        line=dict(color='#9B59B6', width=1.5),
        name='ΔGas'
    ))
    fig_tasa.add_hline(y=0, line_dash='dot', line_color='gray')
    fig_tasa.update_layout(height=250, margin=dict(t=10))
    st.plotly_chart(fig_tasa, use_container_width=True)

    tasa_clean = tasa.dropna()
    if not tasa_clean.empty:
        idx_max = tasa_clean.idxmax()
        idx_min = tasa_clean.idxmin()
        label_max = idx_max.strftime('%H:%M:%S') if isinstance(idx_max, pd.Timestamp) else str(idx_max)
        label_min = idx_min.strftime('%H:%M:%S') if isinstance(idx_min, pd.Timestamp) else str(idx_min)
        st.info(f'⬆️ Mayor subida: `{label_max}` → +{tasa_clean[idx_max]:.2f} PPM')
        st.info(f'⬇️ Mayor bajada: `{label_min}` → {tasa_clean[idx_min]:.2f} PPM')

# ── Tab 2: Estadísticas ────────────────────────────────────────────────
with tab2:
    st.subheader('Estadísticos descriptivos')
    desc = serie.describe()

    c1, c2, c3 = st.columns(3)
    c1.metric('Media',     f'{desc["mean"]:.2f} PPM')
    c1.metric('Mediana',   f'{serie.median():.2f} PPM')
    c2.metric('Máximo',    f'{desc["max"]:.2f} PPM')
    c2.metric('Mínimo',    f'{desc["min"]:.2f} PPM')
    c3.metric('Desv. std', f'{desc["std"]:.2f} PPM')
    c3.metric('IQR',       f'{(desc["75%"] - desc["25%"]):.2f} PPM')

    st.divider()
    st.subheader('Distribución')
    fig_hist = px.histogram(serie, nbins=30, color_discrete_sequence=['#E74C3C'])
    fig_hist.add_vline(x=serie.mean(),   line_dash='dash', line_color='black',
                       annotation_text='Media')
    fig_hist.add_vline(x=serie.median(), line_dash='dash', line_color='green',
                       annotation_text='Mediana')
    st.plotly_chart(fig_hist, use_container_width=True)

# ── Tab 3: Filtros ─────────────────────────────────────────────────────
with tab3:
    st.subheader('Filtrar por rango de valores')
    mn, mx = float(serie.min()), float(serie.max())

    if mn == mx:
        st.warning('Todos los valores son iguales — sin variación para filtrar.')
        st.dataframe(serie)
    else:
        rango = st.slider('Rango PPM', mn, mx, (mn, mx))
        filtrado = serie[(serie >= rango[0]) & (serie <= rango[1])]
        st.write(f'{len(filtrado)} registros en el rango seleccionado')
        st.line_chart(filtrado)

        csv = filtrado.reset_index().to_csv(index=False).encode('utf-8')
        st.download_button('⬇️ Descargar CSV filtrado', csv,
                           'gas_filtrado.csv', 'text/csv')

# ── Tab 4: Mapa ────────────────────────────────────────────────────────
with tab4:
    mapa_df = pd.DataFrame({'lat': [6.2006], 'lon': [-75.5783]})
    st.map(mapa_df, zoom=15)
    st.write('**Universidad EAFIT** · Medellín, Colombia')
    st.write('Sensor: ESP32 + MQ · Protocolo: MQTT → InfluxDB Cloud')

# ══════════════════════════════════════════════════════════════════════
#  AUTO-REFRESH
# ══════════════════════════════════════════════════════════════════════
if auto_ref:
    st.caption('🔄 Actualizando en 30 segundos...')
    time.sleep(30)
    st.rerun()

# ── Footer ─────────────────────────────────────────────────────────────
st.divider()
st.caption('Monitor de Pipeta de Gas · EAFIT · Medellín, Colombia')
