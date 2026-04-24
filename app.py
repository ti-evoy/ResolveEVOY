from __future__ import annotations

import hashlib
import hmac
import io
import re
from datetime import date, datetime

import gspread
import pandas as pd
import plotly.express as px
import streamlit as st
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials


st.set_page_config(
    page_title="Gestao de Reclamacoes",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)


CANAIS = ["RECLAME AQUI", "CENTRAL DE ATENDIMENTO"]
RETORNO_OPT = ["SEM RETORNO", "SIM", "NAO", "NAO CONCLUSIVO"]
STATUS_OPT = ["ABERTO", "EM ANDAMENTO", "FINALIZADO"]
ROLE_OPTIONS = ["EDITOR", "VISUALIZADOR"]
ACCESS_HEADERS = ["id", "email", "senha", "perfil"]
DASHBOARD_FILTERS_VERSION = 3
DATA_LOGIC_VERSION = 3
COLUNAS = [
    "PROTOCOLO",
    "DATA",
    "CANAL",
    "NOME DO CLIENTE",
    "CPF",
    "MOTIVO",
    "REPRESENTANTE",
    "RESPONSAVEL INTERNO",
    "S.A",
    "STATUS",
    "DATA RETORNO",
    "DEVOLUCAO",
    "RETORNO DO REPRESENTANTE",
    "OBSERVACAO",
    "LOG ALTERACOES",
]


def get_session_secret() -> str:
    return str(st.secrets.get("access_token", st.secrets.get("sheet_id", "resolvevoy"))).strip()


def build_auth_signature(email: str) -> str:
    return hmac.new(
        get_session_secret().encode("utf-8"),
        email.strip().lower().encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def normalize_role(value: str) -> str:
    role = str(value).strip().upper()
    if role not in ROLE_OPTIONS:
        return "VISUALIZADOR"
    return role


def normalize_status(value: str) -> str:
    status = str(value).strip().upper()
    if status not in STATUS_OPT:
        return "ABERTO"
    return status


def current_user_role() -> str:
    return normalize_role(st.session_state.get("usuario_perfil", "VISUALIZADOR"))


def can_edit() -> bool:
    return current_user_role() == "EDITOR"


def check_login() -> bool:
    if st.session_state.get("autenticado"):
        return True

    restored_user = restore_login_from_query()
    if restored_user is not None:
        st.session_state["autenticado"] = True
        st.session_state["usuario_email"] = restored_user["email"]
        st.session_state["usuario_id"] = restored_user["id"]
        st.session_state["usuario_perfil"] = normalize_role(restored_user.get("perfil", "VISUALIZADOR"))
        return True

    st.markdown(
        """
        <style>
        html, body, [data-testid="stAppViewContainer"], .stApp{height:100vh;overflow:hidden!important;}
        [data-testid="stAppViewContainer"] > .main{height:100vh;}
        .main .block-container{max-width:460px;padding-top:3vh;padding-bottom:0;min-height:100vh;
            display:flex;flex-direction:column;justify-content:center;}
        header[data-testid="stHeader"]{background:transparent;}
        .login-shell{padding:0;
            background:
            radial-gradient(circle at top left, rgba(124,58,237,0.20), transparent 28%),
            radial-gradient(circle at bottom right, rgba(37,99,235,0.18), transparent 26%),
            linear-gradient(180deg,#0b0d14 0%,#0f1117 100%);}
        .login-card{background:linear-gradient(145deg,rgba(30,32,53,0.96) 0%,rgba(20,23,38,0.98) 100%);
            border:1px solid rgba(99,102,241,0.22);box-shadow:0 24px 70px rgba(0,0,0,0.38);
            border-radius:28px;padding:34px 30px 26px 30px;text-align:center;}
        .login-badge{width:72px;height:72px;border-radius:22px;margin:0 auto 18px auto;
            display:flex;align-items:center;justify-content:center;font-size:2rem;
            background:linear-gradient(135deg,#312e81 0%,#4f46e5 55%,#2563eb 100%);
            box-shadow:0 12px 32px rgba(79,70,229,0.35);}
        .login-card h2{color:#eef2ff;font-size:1.7rem;font-weight:800;margin:0 0 8px 0;}
        .login-card p{color:#a5b4fc;font-size:0.95rem;line-height:1.5;margin:0 0 10px 0;}
        .login-note{color:#7b82a8;font-size:0.8rem;margin-top:8px;}
        .login-form [data-testid="stTextInput"] label{color:#c7d2fe!important;font-weight:600!important;}
        .login-form .stButton>button{height:44px;}
        .login-form{margin-top:-6px;}
        </style>
        <div class="login-shell">
        <div class="login-card">
            <div class="login-badge">🔐</div>
            <h2>Acesso Restrito</h2>
            <p>Entre com seu email e senha para continuar usando o painel.</p>
            <div class="login-note">O acesso fica lembrado neste navegador mesmo apos atualizar a pagina.</div>
        </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _, center, _ = st.columns([1.2, 1, 1.2])
    with center:
        st.markdown('<div class="login-form">', unsafe_allow_html=True)
        access_df = load_access_data(DATA_LOGIC_VERSION)
        if access_df.empty:
            st.info("Primeiro acesso: crie o usuario inicial.")
            init_email = st.text_input("Email inicial", placeholder="voce@empresa.com")
            init_password = st.text_input("Senha inicial", type="password", placeholder="Digite a senha")
            init_password_confirm = st.text_input("Confirmar senha", type="password", placeholder="Repita a senha")
            if st.button("Criar primeiro acesso", width="stretch"):
                erros = []
                if not init_email.strip():
                    erros.append("Email e obrigatorio.")
                if not init_password:
                    erros.append("Senha e obrigatoria.")
                if init_password != init_password_confirm:
                    erros.append("As senhas nao conferem.")
                if erros:
                    for erro in erros:
                        st.error(erro)
                else:
                    create_access(init_email, init_password, "EDITOR")
                    user = authenticate_user(init_email, init_password)
                    if user is not None:
                        st.session_state["autenticado"] = True
                        st.session_state["usuario_email"] = user["email"]
                        st.session_state["usuario_id"] = user["id"]
                        st.session_state["usuario_perfil"] = normalize_role(user.get("perfil", "EDITOR"))
                        persist_login(user["email"])
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
            return False

        email_input = st.text_input("Email", placeholder="voce@empresa.com")
        senha_input = st.text_input(
            "Senha",
            type="password",
            placeholder="••••••••",
        )
        entrar = st.button("Entrar", width="stretch")

        if entrar:
            user = authenticate_user(email_input, senha_input)
            if user is not None:
                st.session_state["autenticado"] = True
                st.session_state["usuario_email"] = user["email"]
                st.session_state["usuario_id"] = user["id"]
                st.session_state["usuario_perfil"] = normalize_role(user.get("perfil", "VISUALIZADOR"))
                persist_login(user["email"])
                st.rerun()
            else:
                st.error("Email ou senha incorretos.")
        st.markdown("</div>", unsafe_allow_html=True)
    return False


st.markdown(
    """
    <style>
    html,body,[class*="css"]{font-family:"Segoe UI","Trebuchet MS",sans-serif;}
    .stApp{background:#0f1117;}
    [data-testid="stSidebar"]{background:linear-gradient(160deg,#13151f 0%,#1a1d2e 100%);border-right:1px solid #2a2d3e;}
    [data-testid="stSidebar"] .stRadio label{color:#c8cde8!important;}
    .metric-card{background:linear-gradient(135deg,#1e2035 0%,#252842 100%);border:1px solid #2e3154;border-radius:16px;padding:18px 20px;margin:4px 0;min-height:100px;}
    .metric-label{color:#7b82a8;font-size:0.72rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;margin-bottom:6px;}
    .metric-value{color:#e8eaff;font-size:1.55rem;font-weight:700;font-family:"Consolas","Courier New",monospace;line-height:1.3;}
    .metric-value-md{color:#e8eaff;font-size:1.1rem;font-weight:700;font-family:"Consolas","Courier New",monospace;line-height:1.4;word-break:break-word;}
    .metric-sub{color:#5a6080;font-size:0.72rem;margin-top:4px;}
    .section-title{color:#a0a8d0;font-size:0.72rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;margin:28px 0 12px 0;padding-bottom:8px;border-bottom:1px solid #2a2d3e;}
    .tag-reclame{background:#2d1b4e;color:#c084fc;border:1px solid #7c3aed;border-radius:20px;padding:2px 10px;font-size:0.72rem;font-weight:600;}
    .tag-central{background:#1b3050;color:#60a5fa;border:1px solid #2563eb;border-radius:20px;padding:2px 10px;font-size:0.72rem;font-weight:600;}
    .tag-sim{background:#1a3a2a;color:#4ade80;border:1px solid #16a34a;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
    .tag-nao{background:#3a1a1a;color:#f87171;border:1px solid #dc2626;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
    .tag-nc{background:#3a2a10;color:#fbbf24;border:1px solid #d97706;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
    .tag-sr{background:#252535;color:#94a3b8;border:1px solid #475569;border-radius:20px;padding:2px 10px;font-size:0.72rem;}
    .tag-aberto{background:#3a1a1a;color:#fca5a5;border:1px solid #ef4444;border-radius:20px;padding:2px 10px;font-size:0.72rem;font-weight:600;}
    .tag-andamento{background:#3a2a10;color:#fcd34d;border:1px solid #f59e0b;border-radius:20px;padding:2px 10px;font-size:0.72rem;font-weight:600;}
    .tag-finalizado{background:#1a3a2a;color:#86efac;border:1px solid #22c55e;border-radius:20px;padding:2px 10px;font-size:0.72rem;font-weight:600;}
    .stTextInput>div>div>input,.stSelectbox>div>div>div,.stDateInput>div>div>input{background:#12141f!important;border:1px solid #2e3154!important;border-radius:10px!important;color:#e0e4ff!important;}
    .stTextArea>div>div>textarea{background:#12141f!important;border:1px solid #2e3154!important;border-radius:10px!important;color:#e0e4ff!important;}
    .stButton>button{background:linear-gradient(135deg,#4f46e5 0%,#7c3aed 100%)!important;color:white!important;border:none!important;border-radius:10px!important;font-weight:600!important;padding:10px 28px!important;transition:opacity 0.2s!important;}
    .stButton>button:hover{opacity:0.88!important;}
    .page-header{background:linear-gradient(135deg,#1e2035 0%,#252842 100%);border:1px solid #2e3154;border-radius:16px;padding:24px 32px;margin-bottom:24px;}
    .page-header h1{color:#e8eaff;font-size:1.6rem;font-weight:700;margin:0;}
    .page-header p{color:#7b82a8;font-size:0.85rem;margin:4px 0 0 0;}
    .search-card{background:#1e2035;border:1px solid #2e3154;border-radius:14px;padding:20px 24px;margin:10px 0;}
    .edit-card{background:#181a2e;border:2px solid #4f46e5;border-radius:14px;padding:20px 24px;margin:4px 0 12px 0;}
    .stCheckbox label{color:#c8cde8!important;}
    div[data-testid="stHorizontalBlock"]{gap:16px;}
    </style>
    """,
    unsafe_allow_html=True,
)


def fmt_doc(value: str) -> str:
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 11:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
    if len(digits) == 14:
        return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
    return str(value)


def to_float(value: str) -> float:
    text = str(value).strip()
    if re.match(r"^\d{1,3}(\.\d{3})*(,\d{2})?$", text):
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def fmt_brl(value: float) -> str:
    try:
        number = float(value)
        text = f"{number:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {text}"
    except (TypeError, ValueError):
        return str(value)


def is_dev(value: str) -> bool:
    return str(value).strip().upper() not in ["NAO", "-", "", "NAN", "NONE"]


def dev_display(value: str) -> str:
    if not is_dev(value):
        return "NAO"
    amount = to_float(value)
    return fmt_brl(amount) if amount > 0 else str(value)


def canal_tag(canal: str) -> str:
    if "RECLAME" in str(canal).upper():
        return '<span class="tag-reclame">🔴 Reclame Aqui</span>'
    return '<span class="tag-central">🔵 Central de Atendimento</span>'


def retorno_tag(retorno: str) -> str:
    item = str(retorno).strip().upper()
    if item == "SIM":
        return '<span class="tag-sim">✅ SIM</span>'
    if item == "NAO":
        return '<span class="tag-nao">❌ NAO</span>'
    if "CONCLUSIVO" in item:
        return '<span class="tag-nc">⚠️ NAO CONCLUSIVO</span>'
    return '<span class="tag-sr">- SEM RETORNO</span>'


def status_tag(status: str) -> str:
    item = str(status).strip().upper()
    if item == "FINALIZADO":
        return '<span class="tag-finalizado">FINALIZADO</span>'
    if item == "EM ANDAMENTO":
        return '<span class="tag-andamento">EM ANDAMENTO</span>'
    return '<span class="tag-aberto">ABERTO</span>'


def generate_protocol_from_number(number: int) -> str:
    return f"REC-{int(number):06d}"


def extract_protocol_number(value: str) -> int:
    digits = re.sub(r"\D", "", str(value))
    return int(digits) if digits else 0


def build_log_entry(action: str) -> str:
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    usuario = str(st.session_state.get("usuario_email", "")).strip()
    if usuario:
        return f"[{timestamp}] ({usuario}) {action}"
    return f"[{timestamp}] {action}"


def append_log(existing_log: str, action: str) -> str:
    current = str(existing_log).strip()
    entry = build_log_entry(action)
    if not current:
        return entry
    return f"{current}\n{entry}"


def next_protocol(df: pd.DataFrame) -> str:
    if df.empty or "PROTOCOLO" not in df.columns:
        return generate_protocol_from_number(1)
    max_protocol = max((extract_protocol_number(value) for value in df["PROTOCOLO"].astype(str)), default=0)
    return generate_protocol_from_number(max_protocol + 1)


def count_unique_cases(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    if "PROTOCOLO" in df.columns:
        protocolos = df["PROTOCOLO"].astype(str).str.strip()
        protocolos = protocolos[protocolos.ne("")]
        return int(protocolos.nunique())
    if "_row_id" in df.columns:
        return int(df["_row_id"].nunique())
    return int(len(df))


def unique_cases_by_column(df: pd.DataFrame, column: str, default_label: str = "SEM DADO") -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return pd.DataFrame(columns=[column, "Qtd"])
    temp = df.copy()
    temp[column] = temp[column].astype(str).str.strip()
    temp[column] = temp[column].replace({"": default_label, "nan": default_label, "None": default_label})
    grouped = (
        temp.groupby(column)["PROTOCOLO"]
        .nunique()
        .reset_index(name="Qtd")
        .sort_values("Qtd", ascending=False)
    )
    return grouped


def parse_date_value(value):
    text = str(value).strip()
    if not text or text in {"nan", "None", "NaT"}:
        return pd.NaT
    parsed = pd.to_datetime(text, format="%d/%m/%Y", errors="coerce")
    if pd.notna(parsed):
        return parsed
    parsed = pd.to_datetime(text, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if pd.notna(parsed):
        return parsed
    parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    return parsed


def normalize_date_cell(value) -> str:
    parsed = parse_date_value(value)
    if pd.notna(parsed):
        return parsed.strftime("%d/%m/%Y")
    text = str(value).strip()
    if text in {"nan", "None", "NaT"}:
        return ""
    return text


def next_access_id(df: pd.DataFrame) -> int:
    if df.empty or "id" not in df.columns:
        return 1
    numbers = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
    return int(numbers.max()) + 1


def normalize_row(row: dict) -> dict:
    return {
        "PROTOCOLO": str(row.get("PROTOCOLO", "")).strip().upper(),
        "DATA": normalize_date_cell(row.get("DATA", "")),
        "CANAL": str(row.get("CANAL", "")).strip().upper(),
        "NOME DO CLIENTE": str(row.get("NOME DO CLIENTE", "")).strip().upper(),
        "CPF": fmt_doc(row.get("CPF", "")),
        "MOTIVO": str(row.get("MOTIVO", "")).strip().upper(),
        "REPRESENTANTE": str(row.get("REPRESENTANTE", "")).strip().upper(),
        "RESPONSAVEL INTERNO": str(row.get("RESPONSAVEL INTERNO", "")).strip().upper(),
        "S.A": str(row.get("S.A", "")).strip().upper(),
        "STATUS": normalize_status(row.get("STATUS", "ABERTO")),
        "DATA RETORNO": normalize_date_cell(row.get("DATA RETORNO", "")),
        "DEVOLUCAO": str(row.get("DEVOLUCAO", "")).strip().upper(),
        "RETORNO DO REPRESENTANTE": str(row.get("RETORNO DO REPRESENTANTE", "")).strip().upper(),
        "OBSERVACAO": str(row.get("OBSERVACAO", "")).strip(),
        "LOG ALTERACOES": str(row.get("LOG ALTERACOES", "")).strip(),
    }


@st.cache_resource
def get_spreadsheet():
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sheet_id = str(st.secrets["sheet_id"]).strip()
    return client.open_by_key(sheet_id)


@st.cache_resource
def get_worksheet():
    spreadsheet = get_spreadsheet()
    try:
        worksheet = spreadsheet.worksheet("registros")
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title="registros", rows=2000, cols=len(COLUNAS))

    current_headers = worksheet.row_values(1)
    last_col_letter = chr(ord("A") + len(COLUNAS) - 1)
    if current_headers != COLUNAS:
        if not current_headers:
            worksheet.update(f"A1:{last_col_letter}1", [COLUNAS])
        else:
            existing_values = worksheet.get_all_values()
            if not existing_values:
                worksheet.update(f"A1:{last_col_letter}1", [COLUNAS])
            else:
                old_headers = existing_values[0]
                body = existing_values[1:]
                old_index = {name: idx for idx, name in enumerate(old_headers)}
                rebuilt_rows = []
                for row in body:
                    rebuilt_rows.append(
                        [row[old_index[col]] if col in old_index and old_index[col] < len(row) else "" for col in COLUNAS]
                    )
                worksheet.clear()
                worksheet.update(f"A1:{last_col_letter}1", [COLUNAS])
                if rebuilt_rows:
                    worksheet.update(f"A2:{last_col_letter}{len(rebuilt_rows) + 1}", rebuilt_rows)
    return worksheet


@st.cache_resource
def get_access_worksheet():
    spreadsheet = get_spreadsheet()
    try:
        worksheet = spreadsheet.worksheet("acessos")
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title="acessos", rows=1000, cols=4)

    current_headers = worksheet.row_values(1)
    if current_headers != ACCESS_HEADERS:
        existing_values = worksheet.get_all_values()
        if not existing_values:
            worksheet.update("A1:D1", [ACCESS_HEADERS])
        else:
            old_headers = existing_values[0]
            body = existing_values[1:]
            old_index = {name: idx for idx, name in enumerate(old_headers)}
            rebuilt_rows = []
            for row in body:
                rebuilt = [row[old_index[col]] if col in old_index and old_index[col] < len(row) else "" for col in ACCESS_HEADERS]
                if not rebuilt[3]:
                    rebuilt[3] = "EDITOR"
                rebuilt_rows.append(rebuilt)
            worksheet.clear()
            worksheet.update("A1:D1", [ACCESS_HEADERS])
            if rebuilt_rows:
                worksheet.update(f"A2:D{len(rebuilt_rows) + 1}", rebuilt_rows)
    return worksheet


@st.cache_data(ttl=30, show_spinner=False)
def load_access_data(cache_version: int = DATA_LOGIC_VERSION) -> pd.DataFrame:
    worksheet = get_access_worksheet()
    records = worksheet.get_all_records(expected_headers=ACCESS_HEADERS)
    if not records:
        return pd.DataFrame(columns=ACCESS_HEADERS)
    df = pd.DataFrame(records)
    for col in ACCESS_HEADERS:
        if col not in df.columns:
            df[col] = ""
    df["perfil"] = df["perfil"].apply(normalize_role)
    return df[ACCESS_HEADERS]


def create_access(email: str, senha: str, perfil: str = "VISUALIZADOR") -> None:
    worksheet = get_access_worksheet()
    df = load_access_data(DATA_LOGIC_VERSION)
    new_id = next_access_id(df)
    worksheet.append_row([str(new_id), email.strip().lower(), senha, normalize_role(perfil)], value_input_option="RAW")
    load_access_data.clear()


def find_user_by_email(email: str):
    df = load_access_data(DATA_LOGIC_VERSION)
    if df.empty:
        return None
    users = df[df["email"].astype(str).str.strip().str.lower().eq(email.strip().lower())]
    if users.empty:
        return None
    return users.iloc[0].to_dict()


def authenticate_user(email: str, senha: str):
    df = load_access_data(DATA_LOGIC_VERSION)
    if df.empty:
        return None
    email_norm = email.strip().lower()
    senha_norm = str(senha)
    users = df[
        df["email"].astype(str).str.strip().str.lower().eq(email_norm)
        & df["senha"].astype(str).eq(senha_norm)
    ]
    if users.empty:
        return None
    return users.iloc[0].to_dict()


def persist_login(user_email: str) -> None:
    st.query_params["auth_email"] = user_email.strip().lower()
    st.query_params["auth_sig"] = build_auth_signature(user_email)


def clear_persisted_login() -> None:
    st.query_params.clear()


def restore_login_from_query():
    auth_email = str(st.query_params.get("auth_email", "")).strip().lower()
    auth_sig = str(st.query_params.get("auth_sig", "")).strip()
    if not auth_email or not auth_sig:
        return None
    if auth_sig != build_auth_signature(auth_email):
        return None
    return find_user_by_email(auth_email)


if not check_login():
    st.stop()




def sync_derived_fields(worksheet, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    max_protocol = 0
    for value in df["PROTOCOLO"].astype(str):
        max_protocol = max(max_protocol, extract_protocol_number(value))

    changed_rows: list[tuple[int, list[str]]] = []
    normalized_rows = []
    for _, row in df.iterrows():
        item = row.to_dict()
        original = normalize_row(item)
        protocolo = str(item.get("PROTOCOLO", "")).strip().upper()
        if not protocolo:
            max_protocol += 1
            protocolo = generate_protocol_from_number(max_protocol)
            item["PROTOCOLO"] = protocolo
            item["LOG ALTERACOES"] = append_log(item.get("LOG ALTERACOES", ""), "Protocolo gerado automaticamente.")

        status = str(item.get("STATUS", "")).strip().upper() or "ABERTO"
        if status not in STATUS_OPT:
            status = "ABERTO"
            item["STATUS"] = status

        retorno = str(item.get("DATA RETORNO", "")).strip()
        if status == "FINALIZADO" and not retorno and pd.notna(row.get("DATA")):
            retorno = pd.to_datetime(row["DATA"]).strftime("%d/%m/%Y")
            item["DATA RETORNO"] = retorno

        normalized = normalize_row(item)
        normalized_rows.append(normalized)
        if any(original[col] != normalized[col] for col in COLUNAS):
            changed_rows.append((int(row["_row_id"]), [normalized[col] for col in COLUNAS]))

    result = pd.DataFrame(normalized_rows)
    result.insert(0, "_row_id", df["_row_id"].tolist())
    result["DATA"] = result["DATA"].apply(parse_date_value)
    result["DATA RETORNO"] = result["DATA RETORNO"].apply(parse_date_value)
    return result


@st.cache_data(ttl=30, show_spinner=False)
def load_data(cache_version: int = DATA_LOGIC_VERSION) -> pd.DataFrame:
    worksheet = get_worksheet()
    values = worksheet.get_all_values()
    if not values or len(values) <= 1:
        return pd.DataFrame(columns=["_row_id"] + COLUNAS)

    headers = values[0]
    body = values[1:]
    header_index = {name: idx for idx, name in enumerate(headers)}
    rows = []
    for row in body:
        rebuilt = {}
        for col in COLUNAS:
            idx = header_index.get(col)
            rebuilt[col] = row[idx] if idx is not None and idx < len(row) else ""
        rows.append(rebuilt)

    df = pd.DataFrame(rows, columns=COLUNAS)
    df["STATUS"] = df["STATUS"].apply(normalize_status)
    df["RETORNO DO REPRESENTANTE"] = (
        df["RETORNO DO REPRESENTANTE"]
        .astype(str)
        .replace({"": "SEM RETORNO", "nan": "SEM RETORNO", "None": "SEM RETORNO"})
    )
    df["DEVOLUCAO"] = (
        df["DEVOLUCAO"]
        .astype(str)
        .replace({"": "NAO", "nan": "NAO", "None": "NAO"})
    )
    df.insert(0, "_row_id", [index + 2 for index in range(len(df))])
    df["DATA"] = df["DATA"].apply(parse_date_value)
    df["DATA RETORNO"] = df["DATA RETORNO"].apply(parse_date_value)
    return sync_derived_fields(worksheet, df)

def append_row(row: dict) -> None:
    item = normalize_row(row)
    worksheet = get_worksheet()
    worksheet.append_row(
        [item[col] for col in COLUNAS],
        value_input_option="USER_ENTERED",
    )
    load_data.clear()


def update_row(row_id: int, row: dict) -> None:
    item = normalize_row(row)
    worksheet = get_worksheet()
    target_range = f"A{int(row_id)}:{chr(ord('A') + len(COLUNAS) - 1)}{int(row_id)}"
    worksheet.update(target_range, [[item[col] for col in COLUNAS]])
    load_data.clear()


def delete_row(row_id: int) -> None:
    worksheet = get_worksheet()
    worksheet.delete_rows(int(row_id))
    load_data.clear()


if st.session_state.get("data_logic_version") != DATA_LOGIC_VERSION:
    st.session_state["data_logic_version"] = DATA_LOGIC_VERSION
    load_data.clear()
    load_access_data.clear()


with st.sidebar:
    st.markdown("### 📋 Reclamacoes")
    st.markdown("---")
    usuario_logado = str(st.session_state.get("usuario_email", "")).strip()
    if usuario_logado:
        st.caption(f"Usuario: {usuario_logado}")
        st.caption(f"Perfil: {current_user_role()}")
        if st.button("Sair", width="stretch"):
            clear_persisted_login()
            st.session_state.clear()
            st.rerun()
        st.markdown("---")
    nav_options = ["📊  Dashboard", "🔍  Pesquisar"]
    if can_edit():
        nav_options = ["➕  Novo Registro"] + nav_options
    pagina = st.radio(
        "Nav",
        nav_options,
        label_visibility="collapsed",
    )
    st.markdown("---")
    df_sb = load_data(DATA_LOGIC_VERSION)
    st.markdown(
        f"<div style='color:#7b82a8;font-size:0.8rem'>Base online</div>"
        f"<div style='color:#e8eaff;font-size:1.4rem;font-weight:700;font-family:Consolas'>{count_unique_cases(df_sb)}</div>"
        f"<div style='color:#5a6080;font-size:0.75rem'>protocolos unicos</div>",
        unsafe_allow_html=True,
    )
    st.caption("")


if pagina == "➕  Novo Registro" and can_edit():
    next_id = next_protocol(df_sb)
    st.markdown(
        '<div class="page-header"><h1>➕ Novo Registro</h1><p>Cadastre uma nova reclamacao na base online</p></div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="section-title">Informacoes Gerais</div>', unsafe_allow_html=True)
    c0, c1, c2, c3 = st.columns([1.2, 1, 1, 1])
    with c0:
        st.text_input("Protocolo", value=next_id, disabled=True)
    with c1:
        data_rec = st.date_input("Data", value=date.today(), format="DD/MM/YYYY")
    with c2:
        canal = st.selectbox("Canal", CANAIS)
    with c3:
        sa = st.text_input("S.A", placeholder="Ex: RL GESTAO")

    st.markdown('<div class="section-title">Dados do Cliente</div>', unsafe_allow_html=True)
    c4, c5 = st.columns([2, 1])
    with c4:
        nome = st.text_input("Nome do Cliente", placeholder="Nome completo")
    with c5:
        cpf_raw = st.text_input("CPF / CNPJ", placeholder="000.000.000-00")

    st.markdown('<div class="section-title">Detalhes da Reclamacao</div>', unsafe_allow_html=True)
    motivo = st.text_input("Motivo", placeholder="Ex: GARANTIA DE CONTEMPLACAO")
    representante = st.text_input("Representante", placeholder="Nome do representante responsavel")
    c8, c9 = st.columns([1, 1])
    with c8:
        responsavel_interno = st.text_input("Responsavel Interno", placeholder="Quem acompanha internamente")
    with c9:
        status = st.selectbox("Status", STATUS_OPT, index=0)

    st.markdown('<div class="section-title">Devolucao e Retorno</div>', unsafe_allow_html=True)
    c6, c7, c10 = st.columns([1, 1, 1])
    with c6:
        teve_dev = st.checkbox("Houve devolucao?")
        valor_dev = None
        if teve_dev:
            valor_dev = st.number_input(
                "Valor da devolucao (R$)",
                min_value=0.01,
                step=0.01,
                format="%.2f",
            )
    with c7:
        retorno = st.selectbox("Retorno do Representante", RETORNO_OPT)
    with c10:
        data_retorno = st.date_input(
            "Data de Retorno",
            value=None if status != "FINALIZADO" else date.today(),
            format="DD/MM/YYYY",
        )

    st.markdown('<div class="section-title">Observacao</div>', unsafe_allow_html=True)
    observacao = st.text_area(
        "Observacao (opcional)",
        placeholder="Informacoes adicionais sobre o caso...",
        height=100,
    )

    if st.button("Salvar Registro", width="stretch"):
        erros = []
        if not nome.strip():
            erros.append("Nome do cliente e obrigatorio.")
        if not cpf_raw.strip():
            erros.append("CPF/CNPJ e obrigatorio.")
        if not motivo.strip():
            erros.append("Motivo e obrigatorio.")
        if not representante.strip():
            erros.append("Representante e obrigatorio.")
        if not responsavel_interno.strip():
            erros.append("Responsavel interno e obrigatorio.")
        if not sa.strip():
            erros.append("S.A e obrigatorio.")
        if teve_dev and not valor_dev:
            erros.append("Informe o valor da devolucao.")
        if status == "FINALIZADO" and retorno != "SEM RETORNO" and not data_retorno:
            erros.append("Informe a data de retorno para casos finalizados com retorno.")

        if erros:
            for erro in erros:
                st.error(erro)
        else:
            append_row(
                {
                    "PROTOCOLO": next_id,
                    "DATA": data_rec.strftime("%d/%m/%Y"),
                    "CANAL": canal,
                    "NOME DO CLIENTE": nome,
                    "CPF": cpf_raw,
                    "MOTIVO": motivo,
                    "REPRESENTANTE": representante,
                    "RESPONSAVEL INTERNO": responsavel_interno,
                    "S.A": sa,
                    "STATUS": status,
                    "DATA RETORNO": data_retorno.strftime("%d/%m/%Y") if data_retorno else "",
                    "DEVOLUCAO": f"{valor_dev:.2f}" if teve_dev else "NAO",
                    "RETORNO DO REPRESENTANTE": retorno,
                    "OBSERVACAO": observacao,
                    "LOG ALTERACOES": build_log_entry("Registro criado."),
                }
            )
            st.success(f"Registro **{next_id}** de **{nome.strip().upper()}** salvo com sucesso.")
            st.balloons()

elif pagina == "📊  Dashboard":
    raw_df = load_data(DATA_LOGIC_VERSION).copy()
    total_base = count_unique_cases(raw_df)

    df = raw_df.copy()
    if not df.empty:
        df["STATUS"] = df["STATUS"].apply(normalize_status)
        df["DATA_NORMALIZADA"] = df["DATA"].dt.normalize()

    st.markdown(
        '<div class="page-header"><h1>📊 Dashboard</h1><p>Visao geral das reclamacoes registradas no Google Sheets</p></div>',
        unsafe_allow_html=True,
    )

    valid_dates = df["DATA"].dropna() if not df.empty else pd.Series(dtype="datetime64[ns]")
    min_date = valid_dates.min().date() if not valid_dates.empty else date.today()
    max_date = valid_dates.max().date() if not valid_dates.empty else date.today()

    if st.session_state.get("dash_filters_version") != DASHBOARD_FILTERS_VERSION:
        st.session_state["dash_filters_version"] = DASHBOARD_FILTERS_VERSION
        st.session_state["dash_dt_ini"] = min_date
        st.session_state["dash_dt_fim"] = max_date
        st.session_state["dash_canal_f"] = CANAIS
        st.session_state["dash_status_f"] = STATUS_OPT
        st.session_state["dash_somente_pendentes"] = False

    dt_ini_key = f"dash_dt_ini_v{DASHBOARD_FILTERS_VERSION}"
    dt_fim_key = f"dash_dt_fim_v{DASHBOARD_FILTERS_VERSION}"
    canal_key = f"dash_canal_f_v{DASHBOARD_FILTERS_VERSION}"
    status_key = f"dash_status_f_v{DASHBOARD_FILTERS_VERSION}"
    pendentes_key = f"dash_somente_pendentes_v{DASHBOARD_FILTERS_VERSION}"

    if dt_ini_key not in st.session_state:
        st.session_state[dt_ini_key] = min_date
    if dt_fim_key not in st.session_state:
        st.session_state[dt_fim_key] = max_date
    if canal_key not in st.session_state:
        st.session_state[canal_key] = CANAIS
    if status_key not in st.session_state:
        st.session_state[status_key] = STATUS_OPT
    if pendentes_key not in st.session_state:
        st.session_state[pendentes_key] = False

    if st.session_state[dt_ini_key] < min_date or st.session_state[dt_ini_key] > max_date:
        st.session_state[dt_ini_key] = min_date
    if st.session_state[dt_fim_key] < min_date or st.session_state[dt_fim_key] > max_date:
        st.session_state[dt_fim_key] = max_date

    st.caption(f"Base carregada: {total_base} protocolos unicos")
    st.caption(
        "Periodo total da base: "
        f"{min_date.strftime('%d/%m/%Y')} ate {max_date.strftime('%d/%m/%Y')}"
    )

    cf0, cf1, cf2, cf3, cf4, cf5 = st.columns([1, 1, 1, 1.5, 1.5, 1.2])
    with cf0:
        if st.button("Limpar filtros", width="stretch"):
            st.session_state[dt_ini_key] = min_date
            st.session_state[dt_fim_key] = max_date
            st.session_state[canal_key] = CANAIS
            st.session_state[status_key] = STATUS_OPT
            st.session_state[pendentes_key] = False
            st.rerun()
    with cf1:
        dt_ini = st.date_input("De", key=dt_ini_key, format="DD/MM/YYYY")
    with cf2:
        dt_fim = st.date_input("Ate", key=dt_fim_key, format="DD/MM/YYYY")
    with cf3:
        canal_f = st.multiselect("Canal", CANAIS, key=canal_key)
    with cf4:
        status_f = st.multiselect("Status", STATUS_OPT, key=status_key)
    with cf5:
        somente_pendentes = st.checkbox("So pendentes", key=pendentes_key)

    if df.empty:
        df_f = df
    else:
        mask = (
            (
                df["DATA"].isna()
                | (
                    (df["DATA"].dt.date >= dt_ini)
                    & (df["DATA"].dt.date <= dt_fim)
                )
            )
            & (df["CANAL"].isin(canal_f))
            & (df["STATUS"].isin(status_f))
        )
        if somente_pendentes:
            mask = mask & (df["STATUS"] != "FINALIZADO")
        df_f = df[mask].copy()

    st.caption(
        f"Debug: base total = {count_unique_cases(raw_df)} protocolos | "
        f"apos filtros = {count_unique_cases(df_f)} protocolos"
    )

    st.markdown('<div class="section-title">Indicadores</div>', unsafe_allow_html=True)
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    total = count_unique_cases(df_f)
    rec_aq = int(df_f.loc[df_f["CANAL"] == "RECLAME AQUI", "PROTOCOLO"].nunique()) if total else 0
    abertos = int(df_f.loc[df_f["STATUS"].astype(str).str.upper() == "ABERTO", "PROTOCOLO"].nunique()) if total else 0
    andamento = int(df_f.loc[df_f["STATUS"].astype(str).str.upper() == "EM ANDAMENTO", "PROTOCOLO"].nunique()) if total else 0
    finalizados = int(df_f.loc[df_f["STATUS"].astype(str).str.upper() == "FINALIZADO", "PROTOCOLO"].nunique()) if total else 0
    dev_msk = df_f["DEVOLUCAO"].astype(str).apply(is_dev) if total else pd.Series(dtype=bool)
    dev_tot = df_f.loc[dev_msk].drop_duplicates(subset=["PROTOCOLO"])["DEVOLUCAO"].apply(to_float).sum() if total else 0

    for col, label, val, val_class, sub in [
        (k1, "Total Reclamacoes", str(total_base), "metric-value", "mesmo total da base online"),
        (k2, "Reclame Aqui", str(rec_aq), "metric-value", f"{rec_aq / total * 100:.0f}% do total" if total else ""),
        (k3, "Casos Abertos", str(abertos), "metric-value", f"{abertos / total * 100:.0f}% do total" if total else ""),
        (k4, "Em Andamento", str(andamento), "metric-value", f"{andamento / total * 100:.0f}% do total" if total else ""),
        (k5, "Finalizados", str(finalizados), "metric-value", f"{finalizados / total * 100:.0f}% do total" if total else ""),
        (k6, "Total Devolucoes", fmt_brl(dev_tot), "metric-value-md", f"{dev_msk.sum()} casos" if total else ""),
    ]:
        col.markdown(
            f"""<div class="metric-card">
                <div class="metric-label">{label}</div>
                <div class="{val_class}">{val}</div>
                <div class="metric-sub">{sub}</div>
            </div>""",
            unsafe_allow_html=True,
        )

    if total == 0:
        st.info("Nenhum registro encontrado para os filtros selecionados.")
    else:
        st.markdown('<div class="section-title">Resumo Complementar</div>', unsafe_allow_html=True)
        x1, x2 = st.columns(2)
        with x1:
            tempo_medio = 0.0
            df_time = df_f.dropna(subset=["DATA RETORNO"]).copy()
            if not df_time.empty:
                tempo_medio = ((df_time["DATA RETORNO"] - df_time["DATA"]).dt.days.clip(lower=0)).mean()
            st.markdown(
                f"""<div class="metric-card">
                    <div class="metric-label">Tempo Medio de Retorno</div>
                    <div class="metric-value">{tempo_medio:.1f} dias</div>
                    <div class="metric-sub">casos com data de retorno</div>
                </div>""",
                unsafe_allow_html=True,
            )
        with x2:
            top_resp = "-"
            if total:
                serie_resp = df_f["RESPONSAVEL INTERNO"].astype(str).str.strip()
                if not serie_resp.empty and serie_resp.ne("").any():
                    top_resp = serie_resp[serie_resp.ne("")].value_counts().idxmax()
            st.markdown(
                f"""<div class="metric-card">
                    <div class="metric-label">Responsavel com Mais Casos</div>
                    <div class="metric-value-md">{top_resp}</div>
                    <div class="metric-sub">base filtrada</div>
                </div>""",
                unsafe_allow_html=True,
            )

        g1, g2 = st.columns(2)
        with g1:
            st.markdown('<div class="section-title">Reclamacoes por Canal</div>', unsafe_allow_html=True)
            cc = unique_cases_by_column(df_f, "CANAL", "SEM CANAL")
            cc.columns = ["Canal", "Qtd"]
            fig = px.pie(cc, names="Canal", values="Qtd", color_discrete_sequence=["#7c3aed", "#2563eb"], hole=0.55)
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="#c8cde8", margin=dict(t=10, b=10, l=10, r=10))
            fig.update_traces(textfont_color="#fff")
            st.plotly_chart(fig, width="stretch")

        with g2:
            st.markdown('<div class="section-title">Status dos Casos</div>', unsafe_allow_html=True)
            rc = unique_cases_by_column(df_f.assign(STATUS=df_f["STATUS"].astype(str).str.upper()), "STATUS", "ABERTO")
            rc.columns = ["Status", "Qtd"]
            cores = {"ABERTO": "#ef4444", "EM ANDAMENTO": "#f59e0b", "FINALIZADO": "#22c55e"}
            fig2 = px.bar(rc, x="Status", y="Qtd", text="Qtd", color="Status", color_discrete_map=cores)
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8cde8",
                showlegend=False,
                margin=dict(t=10, b=10, l=10, r=10),
                xaxis=dict(gridcolor="#2a2d3e"),
                yaxis=dict(gridcolor="#2a2d3e"),
            )
            fig2.update_traces(textposition="outside", textfont_color="#fff")
            st.plotly_chart(fig2, width="stretch")

        g3, g4 = st.columns(2)
        with g3:
            st.markdown('<div class="section-title">Reclamacoes por Dia</div>', unsafe_allow_html=True)
            df_daily = df_f.dropna(subset=["DATA"]).drop_duplicates(subset=["PROTOCOLO"]).copy()
            daily = (
                df_daily.groupby(df_daily["DATA"].dt.date)["PROTOCOLO"]
                .nunique()
                .reset_index(name="Qtd")
            )
            daily.columns = ["Data", "Qtd"]
            if daily.empty:
                st.info("Sem datas validas para montar o grafico por dia.")
            else:
                fig3 = px.line(daily, x="Data", y="Qtd", markers=True, color_discrete_sequence=["#818cf8"])
                fig3.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#c8cde8",
                    margin=dict(t=10, b=10, l=10, r=10),
                    xaxis=dict(gridcolor="#2a2d3e", tickformat="%d/%m"),
                    yaxis=dict(gridcolor="#2a2d3e"),
                )
                st.plotly_chart(fig3, width="stretch")

        with g4:
            st.markdown('<div class="section-title">Top Responsavel Interno</div>', unsafe_allow_html=True)
            sc = unique_cases_by_column(df_f, "RESPONSAVEL INTERNO", "SEM RESPONSAVEL").head(10)
            sc.columns = ["Responsavel", "Qtd"]
            fig4 = px.bar(sc, x="Qtd", y="Responsavel", orientation="h", text="Qtd", color="Qtd", color_continuous_scale=["#312e81", "#818cf8"])
            fig4.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c8cde8",
                showlegend=False,
                margin=dict(t=10, b=10, l=10, r=10),
                xaxis=dict(gridcolor="#2a2d3e"),
                yaxis=dict(gridcolor="#2a2d3e"),
                coloraxis_showscale=False,
            )
            fig4.update_traces(textposition="outside", textfont_color="#fff")
            st.plotly_chart(fig4, width="stretch")

        st.markdown('<div class="section-title">Registros Recentes</div>', unsafe_allow_html=True)
        df_show = df_f.copy()
        if "DATA" in df_show.columns:
            df_show["_sort_data"] = df_show["DATA"].fillna(pd.Timestamp.min)
            df_show = df_show.sort_values("_sort_data", ascending=False).drop(columns=["_sort_data"])
        df_show = df_show.head(20).drop(columns=["_row_id", "LOG ALTERACOES", "DATA_NORMALIZADA"], errors="ignore").copy()
        df_show["DATA"] = df_show["DATA"].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "")
        df_show["DATA RETORNO"] = df_show["DATA RETORNO"].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "")
        st.dataframe(df_show, width="stretch", hide_index=True)

        st.markdown('<div class="section-title">Exportar</div>', unsafe_allow_html=True)
        df_exp = df_f.drop(columns=["_row_id", "DATA_NORMALIZADA"], errors="ignore").copy()
        df_exp["DATA"] = df_exp["DATA"].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "")
        df_exp["DATA RETORNO"] = df_exp["DATA RETORNO"].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "")
        df_exp["DEVOLUCAO"] = df_exp["DEVOLUCAO"].apply(lambda x: fmt_brl(to_float(x)) if is_dev(x) else "NAO")
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_exp.to_excel(writer, index=False, sheet_name="Reclamacoes")
        buf.seek(0)

        df_exp_full = raw_df.drop(columns=["_row_id"], errors="ignore").copy()
        df_exp_full["DATA"] = df_exp_full["DATA"].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "")
        df_exp_full["DATA RETORNO"] = df_exp_full["DATA RETORNO"].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "")
        df_exp_full["DEVOLUCAO"] = df_exp_full["DEVOLUCAO"].apply(lambda x: fmt_brl(to_float(x)) if is_dev(x) else "NAO")
        buf_full = io.BytesIO()
        with pd.ExcelWriter(buf_full, engine="openpyxl") as writer:
            df_exp_full.to_excel(writer, index=False, sheet_name="Reclamacoes")
        buf_full.seek(0)

        e1, e2 = st.columns(2)
        with e1:
            st.download_button(
                "Exportar dados filtrados (.xlsx)",
                data=buf,
                file_name="reclamacoes_exportado.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )
        with e2:
            st.download_button(
                "Exportar base completa (.xlsx)",
                data=buf_full,
                file_name="reclamacoes_base_completa.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )

elif pagina == "🔍  Pesquisar":
    st.markdown(
        '<div class="page-header"><h1>🔍 Pesquisar</h1><p>Busque registros por protocolo, CPF/CNPJ, data, canal, status, responsavel ou S.A</p></div>',
        unsafe_allow_html=True,
    )

    df = load_data(DATA_LOGIC_VERSION).copy()
    if not df.empty:
        df["DATA"] = df["DATA"].dt.normalize()
        df["DATA RETORNO"] = pd.to_datetime(df["DATA RETORNO"], dayfirst=True, errors="coerce")

    st.markdown('<div class="section-title">Filtros de Busca</div>', unsafe_allow_html=True)
    f1, f2, f3, f4, f5 = st.columns([1.2, 1, 1, 1.2, 1.2])
    with f1:
        termo = st.text_input("Protocolo / CPF", placeholder="Digite para buscar...")
    with f2:
        dt_i = st.date_input("De", value=None, key="si", format="DD/MM/YYYY")
    with f3:
        dt_f_v = st.date_input("Ate", value=None, key="sf", format="DD/MM/YYYY")
    with f4:
        canal_s = st.selectbox("Canal", ["Todos"] + CANAIS)
    with f5:
        status_s = st.selectbox("Status", ["Todos"] + STATUS_OPT + ["PENDENTES"])
    f6, f7, _ = st.columns([1.5, 1.5, 2])
    with f6:
        sa_opts = ["Todos"] + sorted(df["S.A"].dropna().astype(str).str.strip().unique().tolist())
        sa_s = st.selectbox("S.A", sa_opts)
    with f7:
        resp_opts = ["Todos"] + sorted(df["RESPONSAVEL INTERNO"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist())
        resp_s = st.selectbox("Responsavel", resp_opts)

    res = df.copy()
    filtrou = False
    if termo.strip():
        filtrou = True
        termo_upper = termo.strip().upper()
        digits = re.sub(r"\D", "", termo)
        res["_cpf_digits"] = res["CPF"].astype(str).apply(lambda item: re.sub(r"\D", "", item))
        res = res[
            res["PROTOCOLO"].astype(str).str.upper().str.contains(termo_upper, na=False)
            | (res["_cpf_digits"] == digits)
        ]
    if dt_i:
        filtrou = True
        res = res[res["DATA"].dt.date >= dt_i]
    if dt_f_v:
        filtrou = True
        res = res[res["DATA"].dt.date <= dt_f_v]
    if canal_s != "Todos":
        filtrou = True
        res = res[res["CANAL"] == canal_s]
    if status_s == "PENDENTES":
        filtrou = True
        res = res[res["STATUS"] != "FINALIZADO"]
    elif status_s != "Todos":
        filtrou = True
        res = res[res["STATUS"] == status_s]
    if sa_s != "Todos":
        filtrou = True
        res = res[res["S.A"].astype(str).str.strip() == sa_s]
    if resp_s != "Todos":
        filtrou = True
        res = res[res["RESPONSAVEL INTERNO"].astype(str).str.strip() == resp_s]
    if "_cpf_digits" in res.columns:
        res = res.drop(columns=["_cpf_digits"])

    if not filtrou:
        st.markdown(
            """
            <div style="text-align:center;padding:60px 20px;color:#4a5080">
                <div style="font-size:3rem;margin-bottom:16px">🔍</div>
                <div>Use os filtros acima para buscar registros</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    elif res.empty:
        st.warning("Nenhum registro encontrado.")
    else:
        st.success(f"**{len(res)}** registro(s) encontrado(s)")

        for _, row in res.iterrows():
            row_id = int(row["_row_id"])
            data_str = row["DATA"].strftime("%d/%m/%Y") if pd.notna(row["DATA"]) else "-"
            edit_key = f"edit_{row_id}"
            if edit_key not in st.session_state:
                st.session_state[edit_key] = False

            obs_val = str(row.get("OBSERVACAO", "")).strip()
            if obs_val in ["nan", "None", ""]:
                obs_val = "-"

            st.markdown(
                f"""
                <div class="search-card">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
                        <div>
                            <span style="color:#818cf8;font-size:0.82rem;font-weight:700">{row['PROTOCOLO']}</span><br>
                            <span style="color:#e0e4ff;font-size:1.05rem;font-weight:600">{row['NOME DO CLIENTE']}</span>
                            &nbsp;<span style="color:#5a6080;font-size:0.82rem">{row['CPF']}</span>
                        </div>
                        <span style="color:#5a6080;font-size:0.82rem">📅 {data_str}</span>
                    </div>
                    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">
                        {canal_tag(row['CANAL'])} {status_tag(row['STATUS'])} {retorno_tag(row['RETORNO DO REPRESENTANTE'])}
                    </div>
                    <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:12px;margin-bottom:10px">
                        <div><span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Motivo</span><br>
                        <span style="color:#e0e4ff">{row['MOTIVO']}</span></div>
                        <div><span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Representante</span><br>
                        <span style="color:#e0e4ff">{row['REPRESENTANTE']}</span></div>
                        <div><span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Responsavel Interno</span><br>
                        <span style="color:#e0e4ff">{row['RESPONSAVEL INTERNO'] or '-'}</span></div>
                        <div><span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">S.A</span><br>
                        <span style="color:#e0e4ff">{row['S.A']}</span></div>
                    </div>
                    <div style="padding-top:8px;border-top:1px solid #2a2d3e;margin-bottom:6px">
                        <span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Devolucao</span>
                        &nbsp;<span style="color:#e0e4ff;font-weight:600">{dev_display(row['DEVOLUCAO'])}</span>
                    </div>
                    <div style="padding-top:6px;border-top:1px solid #2a2d3e;margin-bottom:6px">
                        <span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Data de Retorno</span>
                        &nbsp;<span style="color:#e0e4ff;font-weight:600">{row['DATA RETORNO'].strftime("%d/%m/%Y") if pd.notna(row['DATA RETORNO']) else '-'}</span>
                    </div>
                    <div style="padding-top:6px;border-top:1px solid #2a2d3e">
                        <span style="color:#7b82a8;font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em">Observacao</span><br>
                        <span style="color:#c8cde8;font-style:italic">{obs_val}</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if can_edit():
                b1, b2, _ = st.columns([1, 1, 6])
                with b1:
                    label = "Fechar" if st.session_state[edit_key] else "Editar"
                    if st.button(label, key=f"btn_edit_{row_id}"):
                        st.session_state[edit_key] = not st.session_state[edit_key]
                        st.rerun()
                with b2:
                    if st.button("Excluir", key=f"btn_del_{row_id}"):
                        st.session_state[f"cdel_{row_id}"] = True

            if can_edit() and st.session_state.get(f"cdel_{row_id}", False):
                st.warning(f"Confirmar exclusao de **{row['NOME DO CLIENTE']}**?")
                cc1, cc2, _ = st.columns([1, 1, 5])
                with cc1:
                    if st.button("Confirmar", key=f"cyes_{row_id}"):
                        delete_row(row_id)
                        st.session_state[f"cdel_{row_id}"] = False
                        st.success("Excluido com sucesso.")
                        st.rerun()
                with cc2:
                    if st.button("Cancelar", key=f"cno_{row_id}"):
                        st.session_state[f"cdel_{row_id}"] = False
                        st.rerun()

            if can_edit() and st.session_state.get(edit_key, False):
                st.markdown('<div class="edit-card">', unsafe_allow_html=True)
                st.markdown("**Editar Registro**")
                st.caption(f"Protocolo: {row['PROTOCOLO']}")
                e1, e2, e3, e10 = st.columns([1, 1, 1, 1])
                with e1:
                    e_data = st.date_input(
                        "Data",
                        value=row["DATA"].date() if pd.notna(row["DATA"]) else date.today(),
                        key=f"ed_{row_id}",
                        format="DD/MM/YYYY",
                    )
                with e2:
                    e_canal = st.selectbox(
                        "Canal",
                        CANAIS,
                        index=CANAIS.index(row["CANAL"]) if row["CANAL"] in CANAIS else 0,
                        key=f"ec_{row_id}",
                    )
                with e3:
                    e_sa = st.text_input("S.A", value=str(row["S.A"]), key=f"esa_{row_id}")
                with e10:
                    status_atual = str(row["STATUS"]).strip().upper()
                    e_status = st.selectbox(
                        "Status",
                        STATUS_OPT,
                        index=STATUS_OPT.index(status_atual) if status_atual in STATUS_OPT else 0,
                        key=f"estatus_{row_id}",
                    )
                e4, e5 = st.columns([2, 1])
                with e4:
                    e_nome = st.text_input("Nome do Cliente", value=str(row["NOME DO CLIENTE"]), key=f"en_{row_id}")
                with e5:
                    e_cpf = st.text_input("CPF / CNPJ", value=str(row["CPF"]), key=f"ecpf_{row_id}")
                e_motivo = st.text_input("Motivo", value=str(row["MOTIVO"]), key=f"em_{row_id}")
                e11, e12 = st.columns([1, 1])
                with e11:
                    e_rep = st.text_input("Representante", value=str(row["REPRESENTANTE"]), key=f"er_{row_id}")
                with e12:
                    e_resp = st.text_input("Responsavel Interno", value=str(row["RESPONSAVEL INTERNO"]), key=f"eri_{row_id}")
                e6, e7, e8 = st.columns([1, 1, 1])
                with e6:
                    tem_dev = is_dev(row["DEVOLUCAO"])
                    e_teve_dev = st.checkbox("Houve devolucao?", value=tem_dev, key=f"edc_{row_id}")
                    e_val_dev = None
                    if e_teve_dev:
                        ini_val = to_float(row["DEVOLUCAO"]) if tem_dev else 0.01
                        if ini_val <= 0:
                            ini_val = 0.01
                        e_val_dev = st.number_input(
                            "Valor (R$)",
                            value=ini_val,
                            min_value=0.01,
                            step=0.01,
                            format="%.2f",
                            key=f"ev_{row_id}",
                        )
                with e7:
                    r_cur = str(row["RETORNO DO REPRESENTANTE"]).strip().upper()
                    r_idx = RETORNO_OPT.index(r_cur) if r_cur in RETORNO_OPT else 0
                    e_ret = st.selectbox(
                        "Retorno do Representante",
                        RETORNO_OPT,
                        index=r_idx,
                        key=f"eret_{row_id}",
                    )
                with e8:
                    e_data_retorno = st.date_input(
                        "Data de Retorno",
                        value=row["DATA RETORNO"].date() if pd.notna(row["DATA RETORNO"]) else None,
                        key=f"edr_{row_id}",
                        format="DD/MM/YYYY",
                    )
                obs_cur = str(row.get("OBSERVACAO", "")).strip()
                if obs_cur in ["nan", "None"]:
                    obs_cur = ""
                e_obs = st.text_area("Observacao", value=obs_cur, height=80, key=f"eobs_{row_id}")
                log_cur = str(row.get("LOG ALTERACOES", "")).strip() or "-"
                st.text_area("Log de Alteracoes", value=log_cur, height=120, disabled=True, key=f"elog_{row_id}")
                s1, s2, _ = st.columns([1.2, 1, 4])
                with s1:
                    if st.button("Salvar alteracoes", key=f"esave_{row_id}"):
                        erros_edicao = []
                        if not e_nome.strip():
                            erros_edicao.append("Nome do cliente e obrigatorio.")
                        if not e_cpf.strip():
                            erros_edicao.append("CPF/CNPJ e obrigatorio.")
                        if not e_motivo.strip():
                            erros_edicao.append("Motivo e obrigatorio.")
                        if not e_rep.strip():
                            erros_edicao.append("Representante e obrigatorio.")
                        if not e_resp.strip():
                            erros_edicao.append("Responsavel interno e obrigatorio.")
                        if not e_sa.strip():
                            erros_edicao.append("S.A e obrigatorio.")
                        if e_status == "FINALIZADO" and e_ret != "SEM RETORNO" and not e_data_retorno:
                            erros_edicao.append("Informe a data de retorno para casos finalizados com retorno.")

                        if erros_edicao:
                            for erro in erros_edicao:
                                st.error(erro)
                        else:
                            novo_log = append_log(row.get("LOG ALTERACOES", ""), "Registro atualizado.")
                            update_row(
                                row_id,
                                {
                                    "PROTOCOLO": row["PROTOCOLO"],
                                    "DATA": pd.Timestamp(e_data).strftime("%d/%m/%Y"),
                                    "CANAL": e_canal,
                                    "NOME DO CLIENTE": e_nome,
                                    "CPF": e_cpf,
                                    "MOTIVO": e_motivo,
                                    "REPRESENTANTE": e_rep,
                                    "RESPONSAVEL INTERNO": e_resp,
                                    "S.A": e_sa,
                                    "STATUS": e_status,
                                    "DATA RETORNO": e_data_retorno.strftime("%d/%m/%Y") if e_data_retorno else "",
                                    "DEVOLUCAO": f"{e_val_dev:.2f}" if e_teve_dev and e_val_dev else "NAO",
                                    "RETORNO DO REPRESENTANTE": e_ret,
                                    "OBSERVACAO": e_obs,
                                    "LOG ALTERACOES": novo_log,
                                },
                            )
                            st.session_state[edit_key] = False
                            st.success("Atualizado com sucesso.")
                            st.rerun()
                with s2:
                    if st.button("Cancelar", key=f"ecancel_{row_id}"):
                        st.session_state[edit_key] = False
                        st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<hr style='border-color:#1e2035;margin:4px 0'>", unsafe_allow_html=True)
