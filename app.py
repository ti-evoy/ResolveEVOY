import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date
import re
import io
import json
import gspread
from google.oauth2.service_account import Credentials

# ── Config ───────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Gestão de Reclamações", page_icon="📋", layout="wide", initial_sidebar_state="expanded")

CANAIS      = ["RECLAME AQUI", "CENTRAL DE ATENDIMENTO"]
RETORNO_OPT = ["SEM RETORNO", "SIM", "NÃO", "NÃO CONCLUSIVO"]
COLUNAS     = ["DATA","CANAL","NOME DO CLIENTE","CPF","MOTIVO","REPRESENTANTE","S.A","DEVOLUÇÃO","RETORNO DO REPRESENTANTE","OBSERVAÇÃO"]
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Sora',sans-serif;}
.stApp{background:#0f1117;}
[data-testid="stSidebar"]{background:linear-gradient(160deg,#13151f 0%,#1a1d2e 100%);border-right:1px solid #2a2d3e;}
[data-testid="stSidebar"] .stRadio label{color:#c8cde8!important;}
.metric-card{background:linear-gradient(135deg,#1e2035 0%,#252842 100%);border:1px solid #2e3154;border-radius:16px;padding:18px 20px;margin:4px 0;min-height:100px;}
.metric-label{color:#7b82a8;font-size:0.72rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;margin-bottom:6px;}
.metric-value{color:#e8eaff;font-size:1.55rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1.3;}
.metric-value-md{color:#e8eaff;font-size:1.1rem;font-weight:700;font-family:'JetBrains Mono',monospace;line-height:1.4;word-break:break-word;}
.metric-sub{color:#5a6080;font-size:0.72rem;margin-top:4px;}
.section-title{color:#a0a8d0;font-size:0.72rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;margin:28px 0 12px 0;padding-bottom:8px;border-bottom:1px solid #2a2d3e;}
.tag-reclame{background:#2d1b4e;color:#c084fc;border:1px solid #7c3aed;border-radius:20px;padding:2px 10px;font-size:0.72rem;font-weight:600;}
.tag-central{background:#1b3050;color:#60a5fa;border:1px solid #2563eb;border-radius:20px;padding:2px 10px;font-size:0.72rem;font-weight:600;}
.tag-sim{background:#1a3a2a;color:#4ade80;border:1px solid #16a34a;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
.tag-nao{background:#3a1a1a;color:#f87171;border:1px solid #dc2626;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
.tag-nc{background:#3a2a10;color:#fbbf24;border:1px solid #d97706;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
.tag-sr{background:#252535;color:#94a3b8;border:1px solid #475569;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
.stTextInput>div>div>input,.stSelectbox>div>div>div,.stDateInput>div>div>input{background:#12141f!important;border:1px solid #2e3154!important;border-radius:10px!important;color:#e0e4ff!important;}
.stTextArea>div>div>textarea{background:#12141f!important;border:1px solid #2e3154!important;border-radius:10px!important;color:#e0e4ff!important;}
.stButton>button{background:linear-gradient(135deg,#4f46e5 0%,#7c3aed 100%)!important;color:white!important;border:none!important;border-radius:10px!important;font-family:'Sora',sans-serif!important;font-weight:600!important;padding:10px 28px!important;transition:opacity 0.2s!important;}
.stButton>button:hover{opacity:0.88!important;}
.page-header{background:linear-gradient(135deg,#1e2035 0%,#252842 100%);border:1px solid #2e3154;border-radius:16px;padding:24px 32px;margin-bottom:24px;}
.page-header h1{color:#e8eaff;font-size:1.6rem;font-weight:700;margin:0;}
.page-header p{color:#7b82a8;font-size:0.85rem;margin:4px 0 0 0;}
.search-card{background:#1e2035;border:1px solid #2e3154;border-radius:14px;padding:20px 24px;margin:10px 0;}
.edit-card{background:#181a2e;border:2px solid #4f46e5;border-radius:14px;padding:20px 24px;margin:4px 0 12px 0;}
.stCheckbox label{color:#c8cde8!important;}
div[data-testid="stHorizontalBlock"]{gap:16px;}
</style>
""", unsafe_allow_html=True)

# ── Google Sheets connection ──────────────────────────────────────────────────
@st.cache_resource
def get_worksheet():
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet_id = st.secrets["sheet_id"]
    sh = client.open_by_key(sheet_id)
    return sh.sheet1

def load_data() -> pd.DataFrame:
    ws = get_worksheet()
    # get_all_records busca TODAS as linhas sem truncar
    try:
        records = ws.get_all_records(numericise_ignore=["all"])
        if not records:
            return pd.DataFrame(columns=COLUNAS)
        df = pd.DataFrame(records)
    except Exception:
        # fallback: leitura por range explícito até última linha
        last_row = len(ws.col_values(1)) + 10
        data = ws.get(f"A1:J{last_row}")
        if len(data) <= 1:
            return pd.DataFrame(columns=COLUNAS)
        df = pd.DataFrame(data[1:], columns=data[0])
    # Garantir todas as colunas
    for col in COLUNAS:
        if col not in df.columns:
            df[col] = ""
    df = df[COLUNAS]
    df["DATA"] = pd.to_datetime(df["DATA"], dayfirst=True, errors="coerce")
    return df

def save_all(df: pd.DataFrame):
    """Sobrescreve a planilha inteira com o DataFrame."""
    ws = get_worksheet()
    df_save = df.copy()
    df_save["DATA"] = df_save["DATA"].apply(
        lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else ""
    )
    df_save = df_save.fillna("").astype(str)
    ws.clear()
    ws.update([df_save.columns.tolist()] + df_save.values.tolist())

def append_row(row: dict):
    """Adiciona uma linha nova no final — mais rápido que reescrever tudo."""
    ws = get_worksheet()
    vals = [row.get(c, "") for c in COLUNAS]
    ws.append_row(vals, value_input_option="USER_ENTERED")

# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_doc(v):
    d = re.sub(r'\D','',str(v))
    if len(d)==11: return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    if len(d)==14: return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return v

def to_float(x):
    s = str(x).strip()
    if re.match(r'^\d{1,3}(\.\d{3})*(,\d{2})?$', s):
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '')
    try: return float(s)
    except: return 0.0

def fmt_brl(valor):
    try:
        v = float(valor)
        s = f"{v:,.2f}".replace(',','X').replace('.', ',').replace('X','.')
        return f"R$ {s}"
    except: return str(valor)

def is_dev(v):
    return str(v).strip().upper() not in ["NÃO","NAO","-","","NAN","NONE"]

def dev_display(v):
    if not is_dev(v): return "NÃO"
    f = to_float(v)
    return fmt_brl(f) if f > 0 else str(v)

def canal_tag(c):
    if "RECLAME" in str(c).upper(): return '<span class="tag-reclame">🔴 Reclame Aqui</span>'
    return '<span class="tag-central">🔵 Central de Atendimento</span>'

def retorno_tag(r):
    r = str(r).strip().upper()
    if r=="SIM": return '<span class="tag-sim">✅ SIM</span>'
    if r in("NÃO","NAO"): return '<span class="tag-nao">❌ NÃO</span>'
    if "CONCLUSIVO" in r: return '<span class="tag-nc">⚠️ NÃO CONCLUSIVO</span>'
    return '<span class="tag-sr">— SEM RETORNO</span>'

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📋 Reclamações")
    st.markdown("---")
    pagina = st.radio("Nav", ["➕  Novo Registro","📊  Dashboard","🔍  Pesquisar"], label_visibility="collapsed")
    st.markdown("---")
    df_sb = load_data()
    st.markdown(f"<div style='color:#7b82a8;font-size:0.8rem'>Base atual</div>"
                f"<div style='color:#e8eaff;font-size:1.4rem;font-weight:700;font-family:JetBrains Mono'>{len(df_sb)}</div>"
                f"<div style='color:#5a6080;font-size:0.75rem'>registros</div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 1 — NOVO REGISTRO
# ══════════════════════════════════════════════════════════════════════════════
if pagina == "➕  Novo Registro":
    st.markdown('<div class="page-header"><h1>➕ Novo Registro</h1><p>Cadastre uma nova reclamação na base de dados</p></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">Informações Gerais</div>', unsafe_allow_html=True)
    c1,c2,c3 = st.columns([1,1,1])
    with c1: data_rec = st.date_input("📅 Data", value=date.today(), format="DD/MM/YYYY")
    with c2: canal    = st.selectbox("📡 Canal", CANAIS)
    with c3: sa       = st.text_input("🏢 S.A", placeholder="Ex: RL GESTÃO")

    st.markdown('<div class="section-title">Dados do Cliente</div>', unsafe_allow_html=True)
    c4,c5 = st.columns([2,1])
    with c4: nome    = st.text_input("👤 Nome do Cliente", placeholder="Nome completo")
    with c5: cpf_raw = st.text_input("🪪 CPF / CNPJ", placeholder="000.000.000-00")

    st.markdown('<div class="section-title">Detalhes da Reclamação</div>', unsafe_allow_html=True)
    motivo        = st.text_input("📌 Motivo", placeholder="Ex: GARANTIA DE CONTEMPLAÇÃO")
    representante = st.text_input("👔 Representante", placeholder="Nome do representante responsável")

    st.markdown('<div class="section-title">Devolução e Retorno</div>', unsafe_allow_html=True)
    c6,c7 = st.columns([1,1])
    with c6:
        teve_dev  = st.checkbox("💰 Houve devolução?")
        valor_dev = None
        if teve_dev:
            valor_dev = st.number_input("Valor da devolução (R$)", min_value=0.01, step=0.01, format="%.2f")
    with c7:
        retorno = st.selectbox("🔄 Retorno do Representante", RETORNO_OPT)

    st.markdown('<div class="section-title">Observação</div>', unsafe_allow_html=True)
    observacao = st.text_area("📝 Observação (opcional)", placeholder="Informações adicionais sobre o caso...", height=100)

    st.markdown("")
    if st.button("💾 Salvar Registro", use_container_width=True):
        erros = []
        if not nome.strip():           erros.append("Nome do cliente é obrigatório.")
        if not cpf_raw.strip():        erros.append("CPF/CNPJ é obrigatório.")
        if not motivo.strip():         erros.append("Motivo é obrigatório.")
        if not representante.strip():  erros.append("Representante é obrigatório.")
        if not sa.strip():             erros.append("S.A é obrigatório.")
        if teve_dev and not valor_dev: erros.append("Informe o valor da devolução.")
        if erros:
            for e in erros: st.error(e)
        else:
            dev_val = f"{valor_dev:.2f}" if teve_dev else "NÃO"
            novo = {
                "DATA":                     data_rec.strftime("%d/%m/%Y"),
                "CANAL":                    canal,
                "NOME DO CLIENTE":          nome.strip().upper(),
                "CPF":                      fmt_doc(cpf_raw),
                "MOTIVO":                   motivo.strip().upper(),
                "REPRESENTANTE":            representante.strip().upper(),
                "S.A":                      sa.strip().upper(),
                "DEVOLUÇÃO":                dev_val,
                "RETORNO DO REPRESENTANTE": retorno,
                "OBSERVAÇÃO":               observacao.strip(),
            }
            append_row(novo)
            st.success(f"✅ Registro de **{nome.upper()}** salvo com sucesso!")
            st.cache_resource.clear()
            st.balloons()

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 2 — DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
elif pagina == "📊  Dashboard":
    df = load_data()
    df = df.dropna(subset=["DATA"])
    df["DATA"] = df["DATA"].dt.normalize()

    st.markdown('<div class="page-header"><h1>📊 Dashboard</h1><p>Visão geral das reclamações registradas</p></div>', unsafe_allow_html=True)

    cf1,cf2,cf3 = st.columns([1,1,2])
    with cf1: dt_ini = st.date_input("De",  value=df["DATA"].min().date() if len(df) else date.today(), format="DD/MM/YYYY")
    with cf2: dt_fim = st.date_input("Até", value=df["DATA"].max().date() if len(df) else date.today(), format="DD/MM/YYYY")
    with cf3: canal_f = st.multiselect("Canal", CANAIS, default=CANAIS)

    mask = (df["DATA"].dt.date >= dt_ini) & (df["DATA"].dt.date <= dt_fim) & (df["CANAL"].isin(canal_f))
    df_f = df[mask]

    st.markdown('<div class="section-title">Indicadores</div>', unsafe_allow_html=True)
    k1,k2,k3,k4,k5 = st.columns(5)
    total   = len(df_f)
    rec_aq  = (df_f["CANAL"]=="RECLAME AQUI").sum()
    cent    = (df_f["CANAL"]=="CENTRAL DE ATENDIMENTO").sum()
    dev_msk = df_f["DEVOLUÇÃO"].astype(str).apply(is_dev)
    dev_tot = df_f[dev_msk]["DEVOLUÇÃO"].apply(to_float).sum()
    ret_sim = (df_f["RETORNO DO REPRESENTANTE"].astype(str).str.strip().str.upper()=="SIM").sum()

    for col,label,val,val_class,sub in [
        (k1,"Total Reclamações",str(total),"metric-value","registros"),
        (k2,"Reclame Aqui",str(rec_aq),"metric-value",f"{rec_aq/total*100:.0f}% do total" if total else ""),
        (k3,"Central Atendimento",str(cent),"metric-value",f"{cent/total*100:.0f}% do total" if total else ""),
        (k4,"Total Devoluções",fmt_brl(dev_tot),"metric-value-md",f"{dev_msk.sum()} casos"),
        (k5,"Retorno Confirmado",str(ret_sim),"metric-value",f"{ret_sim/total*100:.0f}% do total" if total else ""),
    ]:
        col.markdown(f"""<div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="{val_class}">{val}</div>
            <div class="metric-sub">{sub}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("")
    g1,g2 = st.columns(2)
    with g1:
        st.markdown('<div class="section-title">Reclamações por Canal</div>', unsafe_allow_html=True)
        cc = df_f["CANAL"].value_counts().reset_index(); cc.columns=["Canal","Qtd"]
        fig=px.pie(cc,names="Canal",values="Qtd",color_discrete_sequence=["#7c3aed","#2563eb"],hole=0.55)
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",font_color="#c8cde8",margin=dict(t=10,b=10,l=10,r=10))
        fig.update_traces(textfont_color="#fff")
        st.plotly_chart(fig,use_container_width=True)
    with g2:
        st.markdown('<div class="section-title">Retorno do Representante</div>', unsafe_allow_html=True)
        rc=df_f["RETORNO DO REPRESENTANTE"].astype(str).str.strip().str.upper().value_counts().reset_index(); rc.columns=["Retorno","Qtd"]
        cores={"SIM":"#4ade80","NÃO":"#f87171","NÃO CONCLUSIVO":"#fbbf24","SEM RETORNO":"#64748b"}
        fig2=px.bar(rc,x="Retorno",y="Qtd",text="Qtd",color="Retorno",color_discrete_map=cores)
        fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font_color="#c8cde8",showlegend=False,margin=dict(t=10,b=10,l=10,r=10),xaxis=dict(gridcolor="#2a2d3e"),yaxis=dict(gridcolor="#2a2d3e"))
        fig2.update_traces(textposition="outside",textfont_color="#fff")
        st.plotly_chart(fig2,use_container_width=True)

    g3,g4 = st.columns(2)
    with g3:
        st.markdown('<div class="section-title">Reclamações por Dia</div>', unsafe_allow_html=True)
        daily=df_f.groupby(df_f["DATA"].dt.date).size().reset_index(); daily.columns=["Data","Qtd"]
        fig3=px.line(daily,x="Data",y="Qtd",markers=True,color_discrete_sequence=["#818cf8"])
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font_color="#c8cde8",margin=dict(t=10,b=10,l=10,r=10),xaxis=dict(gridcolor="#2a2d3e",tickformat="%d/%m"),yaxis=dict(gridcolor="#2a2d3e"))
        st.plotly_chart(fig3,use_container_width=True)
    with g4:
        st.markdown('<div class="section-title">Top S.A</div>', unsafe_allow_html=True)
        sc=df_f["S.A"].astype(str).str.strip().value_counts().head(10).reset_index(); sc.columns=["S.A","Qtd"]
        fig4=px.bar(sc,x="Qtd",y="S.A",orientation="h",text="Qtd",color="Qtd",color_continuous_scale=["#312e81","#818cf8"])
        fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font_color="#c8cde8",showlegend=False,margin=dict(t=10,b=10,l=10,r=10),xaxis=dict(gridcolor="#2a2d3e"),yaxis=dict(gridcolor="#2a2d3e"),coloraxis_showscale=False)
        fig4.update_traces(textposition="outside",textfont_color="#fff")
        st.plotly_chart(fig4,use_container_width=True)

    st.markdown('<div class="section-title">Registros Recentes</div>', unsafe_allow_html=True)
    df_show=df_f.sort_values("DATA",ascending=False).head(20).copy()
    df_show["DATA"]=df_show["DATA"].dt.strftime("%d/%m/%Y")
    st.dataframe(df_show,use_container_width=True,hide_index=True)

    st.markdown('<div class="section-title">Exportar</div>', unsafe_allow_html=True)
    df_exp=df_f.copy()
    df_exp["DATA"]=df_exp["DATA"].dt.strftime("%d/%m/%Y")
    df_exp["DEVOLUÇÃO"]=df_exp["DEVOLUÇÃO"].apply(lambda x: fmt_brl(to_float(x)) if is_dev(x) else "NÃO")
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine="openpyxl") as w:
        df_exp.to_excel(w,index=False,sheet_name="Reclamações")
    buf.seek(0)
    st.download_button("⬇️ Exportar dados filtrados (.xlsx)",data=buf,file_name="reclamacoes_exportado.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 3 — PESQUISAR
# ══════════════════════════════════════════════════════════════════════════════
elif pagina == "🔍  Pesquisar":
    st.markdown('<div class="page-header"><h1>🔍 Pesquisar</h1><p>Busque, edite ou exclua registros por CPF/CNPJ, data, canal ou S.A</p></div>', unsafe_allow_html=True)

    df = load_data()
    df["DATA"] = df["DATA"].dt.normalize()

    st.markdown('<div class="section-title">Filtros de Busca</div>', unsafe_allow_html=True)
    f1,f2,f3,f4 = st.columns([1.5,1,1,1.5])
    with f1: cpf_s  = st.text_input("🪪 CPF / CNPJ", placeholder="Digite para buscar...")
    with f2: dt_i   = st.date_input("📅 De",  value=None, key="si",  format="DD/MM/YYYY")
    with f3: dt_f_v = st.date_input("📅 Até", value=None, key="sf", format="DD/MM/YYYY")
    with f4: canal_s= st.selectbox("📡 Canal", ["Todos"]+CANAIS)
    f5,_ = st.columns([1.5,3])
    with f5:
        sa_opts=["Todos"]+sorted(df["S.A"].dropna().astype(str).str.strip().unique().tolist())
        sa_s=st.selectbox("🏢 S.A", sa_opts)

    res=df.copy()
    filtrou=False
    if cpf_s.strip():
        filtrou=True
        digs=re.sub(r'\D','',cpf_s)
        res["_d"]=res["CPF"].astype(str).apply(lambda x: re.sub(r'\D','',x))
        res=res[res["_d"]==digs]
    if dt_i:
        filtrou=True; res=res[res["DATA"].dt.date>=dt_i]
    if dt_f_v:
        filtrou=True; res=res[res["DATA"].dt.date<=dt_f_v]
    if canal_s!="Todos":
        filtrou=True; res=res[res["CANAL"]==canal_s]
    if sa_s!="Todos":
        filtrou=True; res=res[res["S.A"].astype(str).str.strip()==sa_s]
    if "_d" in res.columns: res=res.drop(columns=["_d"])

    if not filtrou:
        st.markdown("""<div style="text-align:center;padding:60px 20px;color:#4a5080">
            <div style="font-size:3rem;margin-bottom:16px">🔍</div>
            <div>Use os filtros acima para buscar registros</div>
        </div>""", unsafe_allow_html=True)
    elif res.empty:
        st.warning("⚠️ Nenhum registro encontrado.")
    else:
        st.success(f"✅ **{len(res)}** registro(s) encontrado(s)")

        for orig_idx, row in res.iterrows():
            data_str=row["DATA"].strftime("%d/%m/%Y") if pd.notna(row["DATA"]) else "—"
            edit_key=f"edit_{orig_idx}"
            if edit_key not in st.session_state: st.session_state[edit_key]=False

            obs_val=str(row.get("OBSERVAÇÃO","")).strip()
            if obs_val in ["nan","None",""]: obs_val="—"

            st.markdown(f"""
            <div class="search-card">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
                    <div>
                        <span style="color:#e0e4ff;font-size:1.05rem;font-weight:600">{row['NOME DO CLIENTE']}</span>
                        &nbsp;<span style="color:#5a6080;font-size:0.82rem">{row['CPF']}</span>
                    </div>
                    <span style="color:#5a6080;font-size:0.82rem">📅 {data_str}</span>
                </div>
                <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">
                    {canal_tag(row['CANAL'])} {retorno_tag(row['RETORNO DO REPRESENTANTE'])}
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:10px">
                    <div><span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Motivo</span><br>
                    <span style="color:#e0e4ff">{row['MOTIVO']}</span></div>
                    <div><span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Representante</span><br>
                    <span style="color:#e0e4ff">{row['REPRESENTANTE']}</span></div>
                    <div><span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">S.A</span><br>
                    <span style="color:#e0e4ff">{row['S.A']}</span></div>
                </div>
                <div style="padding-top:8px;border-top:1px solid #2a2d3e;margin-bottom:6px">
                    <span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Devolução</span>
                    &nbsp;<span style="color:#e0e4ff;font-weight:600">{dev_display(row['DEVOLUÇÃO'])}</span>
                </div>
                <div style="padding-top:6px;border-top:1px solid #2a2d3e">
                    <span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Observação</span><br>
                    <span style="color:#c8cde8;font-style:italic">{obs_val}</span>
                </div>
            </div>""", unsafe_allow_html=True)

            b1,b2,_ = st.columns([1,1,6])
            with b1:
                lbl="🔒 Fechar" if st.session_state[edit_key] else "✏️ Editar"
                if st.button(lbl, key=f"btn_edit_{orig_idx}"):
                    st.session_state[edit_key]=not st.session_state[edit_key]; st.rerun()
            with b2:
                if st.button("🗑️ Excluir", key=f"btn_del_{orig_idx}"):
                    st.session_state[f"cdel_{orig_idx}"]=True

            if st.session_state.get(f"cdel_{orig_idx}",False):
                st.warning(f"⚠️ Confirmar exclusão de **{row['NOME DO CLIENTE']}**?")
                cc1,cc2,_ = st.columns([1,1,5])
                with cc1:
                    if st.button("✅ Confirmar", key=f"cyes_{orig_idx}"):
                        df_all=load_data()
                        df_all=df_all.drop(index=orig_idx).reset_index(drop=True)
                        save_all(df_all)
                        st.session_state[f"cdel_{orig_idx}"]=False
                        st.cache_resource.clear(); st.success("Excluído!"); st.rerun()
                with cc2:
                    if st.button("❌ Cancelar", key=f"cno_{orig_idx}"):
                        st.session_state[f"cdel_{orig_idx}"]=False; st.rerun()

            if st.session_state.get(edit_key,False):
                st.markdown('<div class="edit-card">', unsafe_allow_html=True)
                st.markdown("**✏️ Editar Registro**")
                e1,e2,e3=st.columns([1,1,1])
                with e1: e_data =st.date_input("Data",value=row["DATA"].date() if pd.notna(row["DATA"]) else date.today(),key=f"ed_{orig_idx}", format="DD/MM/YYYY")
                with e2: e_canal=st.selectbox("Canal",CANAIS,index=CANAIS.index(row["CANAL"]) if row["CANAL"] in CANAIS else 0,key=f"ec_{orig_idx}")
                with e3: e_sa   =st.text_input("S.A",value=str(row["S.A"]),key=f"esa_{orig_idx}")
                e4,e5=st.columns([2,1])
                with e4: e_nome=st.text_input("Nome do Cliente",value=str(row["NOME DO CLIENTE"]),key=f"en_{orig_idx}")
                with e5: e_cpf =st.text_input("CPF / CNPJ",value=str(row["CPF"]),key=f"ecpf_{orig_idx}")
                e_motivo=st.text_input("Motivo",value=str(row["MOTIVO"]),key=f"em_{orig_idx}")
                e_rep   =st.text_input("Representante",value=str(row["REPRESENTANTE"]),key=f"er_{orig_idx}")
                e6,e7=st.columns([1,1])
                with e6:
                    tem_dev=is_dev(row["DEVOLUÇÃO"])
                    e_teve_dev=st.checkbox("Houve devolução?",value=tem_dev,key=f"edc_{orig_idx}")
                    e_val_dev=None
                    if e_teve_dev:
                        ini_val=to_float(row["DEVOLUÇÃO"]) if tem_dev else 0.01
                        if ini_val<=0: ini_val=0.01
                        e_val_dev=st.number_input("Valor (R$)",value=ini_val,min_value=0.01,step=0.01,format="%.2f",key=f"ev_{orig_idx}")
                with e7:
                    r_cur=str(row["RETORNO DO REPRESENTANTE"]).strip().upper()
                    r_idx=RETORNO_OPT.index(r_cur) if r_cur in RETORNO_OPT else 0
                    e_ret=st.selectbox("Retorno do Representante",RETORNO_OPT,index=r_idx,key=f"eret_{orig_idx}")
                obs_cur=str(row.get("OBSERVAÇÃO","")).strip()
                if obs_cur in ["nan","None"]: obs_cur=""
                e_obs=st.text_area("📝 Observação",value=obs_cur,height=80,key=f"eobs_{orig_idx}")
                s1,s2,_=st.columns([1.2,1,4])
                with s1:
                    if st.button("💾 Salvar alterações",key=f"esave_{orig_idx}"):
                        dev_v=f"{e_val_dev:.2f}" if e_teve_dev and e_val_dev else "NÃO"
                        df_all=load_data()
                        df_all.at[orig_idx,"DATA"]                    =pd.Timestamp(e_data)
                        df_all.at[orig_idx,"CANAL"]                   =e_canal
                        df_all.at[orig_idx,"NOME DO CLIENTE"]         =e_nome.strip().upper()
                        df_all.at[orig_idx,"CPF"]                     =fmt_doc(e_cpf)
                        df_all.at[orig_idx,"MOTIVO"]                  =e_motivo.strip().upper()
                        df_all.at[orig_idx,"REPRESENTANTE"]           =e_rep.strip().upper()
                        df_all.at[orig_idx,"S.A"]                     =e_sa.strip().upper()
                        df_all.at[orig_idx,"DEVOLUÇÃO"]               =dev_v
                        df_all.at[orig_idx,"RETORNO DO REPRESENTANTE"]=e_ret
                        df_all.at[orig_idx,"OBSERVAÇÃO"]              =e_obs.strip()
                        save_all(df_all)
                        st.session_state[edit_key]=False
                        st.cache_resource.clear(); st.success("✅ Atualizado!"); st.rerun()
                with s2:
                    if st.button("✖️ Cancelar",key=f"ecancel_{orig_idx}"):
                        st.session_state[edit_key]=False; st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("<hr style='border-color:#1e2035;margin:4px 0'>", unsafe_allow_html=True)