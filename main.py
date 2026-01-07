# -*- coding: utf-8 -*-
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, time as dt_time
import re
import time
import os
import json
import base64
import binascii

# --- Configurações ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
NOME_ABA = 'Tabela dinâmica 2'

# --- 1. LIMPEZA NINJA DE DADOS ---
def limpar_data_e_hora(valor):
    """
    Remove textos como 'LH - Inbound' e retorna apenas a data/hora.
    """
    valor_str = str(valor).strip()
    if not valor_str or valor_str.lower() in ['nat', 'nan', 'none', '', '--', '-', 'null']:
        return pd.NaT

    try:
        # Regex para DD/MM/AAAA HH:MM
        match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2})', valor_str)
        if match:
            return pd.to_datetime(match.group(1), dayfirst=True)
        
        # Regex para apenas DD/MM/AAAA
        match_data = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', valor_str)
        if match_data:
            return pd.to_datetime(match_data.group(1), dayfirst=True)

        return pd.NaT
    except:
        return pd.NaT

# --- 2. AUXILIARES VISUAIS ---
def formatar_data_visual(data_obj):
    if pd.isna(data_obj): return "--/-- --:--"
    return data_obj.strftime('%d/%m %H:%M')

def minutos_para_hhmm(minutos):
    if minutos is None: return "--:--h"
    if minutos < -1000: minutos = 0 
    sinal = "-" if minutos < 0 else ""
    minutos = abs(minutos)
    horas = minutos // 60
    mins = minutos % 60
    return f"{sinal}{horas:02d}:{mins:02d}h"

def padronizar_doca(doca_str):
    if not isinstance(doca_str, str): return "--"
    match = re.search(r'(\d+)', doca_str)
    return match.group(1) if match else "--"

# --- 3. CONECTIVIDADE ---
def enviar_webhook(mensagem_txt):
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url: return

    mensagem_limpa = str(mensagem_txt).replace('"', "'").replace('\\', '/')
    conteudo = f"```\n{mensagem_limpa}\n```"

    try:
        requests.post(webhook_url, json={"tag": "text", "text": {"format": 1, "content": conteudo}}, timeout=10)
        print("✅ Enviado.")
    except Exception as e:
        print(f"❌ Erro envio: {e}")

def autenticar():
    creds_raw = os.environ.get('GCP_SA_KEY_JSON', '').strip()
    if not creds_raw: return None
    try:
        creds_dict = json.loads(base64.b64decode(creds_raw).decode('utf-8'))
    except:
        creds_dict = json.loads(creds_raw)
    
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# --- 4. LÓGICA DE TEMPO ---
def get_agora_br():
    return (datetime.utcnow() - timedelta(hours=3)).replace(second=0, microsecond=0)

def turno_atual():
    h = get_agora_br().time()
    if h >= dt_time(6,0) and h < dt_time(14,0): return "T1"
    elif h >= dt_time(14,0) and h < dt_time(22,0): return "T2"
    else: return "T3"

def ordenar_turnos(p):
    ordem = ['T1', 'T2', 'T3']
    idx = ordem.index(turno_atual())
    nova = ordem[idx:] + ordem[:idx]
    return sorted([i for i in p.items() if i[0] in nova], key=lambda x: nova.index(x[0]))

def periodo_dia_filtro(agora):
    hoje = agora.date()
    ini = datetime.combine(hoje, dt_time(6,0))
    if agora < ini: ini -= timedelta(days=1)
    fim = ini + timedelta(days=1) - timedelta(seconds=1)
    return ini, fim

# --- MAIN ---
def main():
    print(f"🔄 Script Corrigido (Origem Coluna C).")
    client = autenticar()
    if not client: return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(NOME_ABA)
        raw_data = sheet.get('A1:AC8000')
        print("✅ Dados baixados.")
    except Exception as e:
        print(f"❌ Erro leitura: {e}")
        return

    if not raw_data: return
    df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
    df.columns = [c.strip() for c in df.columns]

    # --- MAPEAMENTO BASEADO NA SUA IMAGEM ---
    # Coluna C (Index 2) = Origem
    # Coluna G (Index 6) = Add to Queue Time (Entrada)
    # Coluna D (Index 3) = Checkin (Chegada)
    
    # Busca segura por nome
    def get_col(nomes, fallback_idx=None):
        for nome in nomes:
            if nome in df.columns: return nome
        if fallback_idx is not None and len(df.columns) > fallback_idx:
            return df.columns[fallback_idx]
        return None

    c_origem = get_col(['Origem'], 2)  # Col C
    c_entrada = get_col(['Add to Queue Time'], 6) # Col G
    c_cheg = get_col(['Checkin', 'Hora Chegada'], 3) # Col D
    c_eta = get_col(['ETA Planejado'], 1) # Col B
    
    # Outras colunas
    c_trip = get_col(['LH Trip Nnumber']) # Col A
    c_status = get_col(['Status 2.0', 'Satus 2.0']) # Col P ou T
    c_doca = get_col(['Doca'])
    c_turno = get_col(['Turno 2'])
    c_pacotes = get_col(['SUM of Pending Inbound Parcel Qty', 'QTD Planejado', 'Pacotes'])

    print(f"🛠️ Colunas: Origem='{c_origem}', Entrada='{c_entrada}', Status='{c_status}'")

    # --- LIMPEZA E CONVERSÃO ---
    if c_entrada: df[c_entrada] = df[c_entrada].apply(limpar_data_e_hora)
    if c_eta: df[c_eta] = df[c_eta].apply(limpar_data_e_hora)
    if c_cheg: df[c_cheg] = df[c_cheg].apply(limpar_data_e_hora)

    # Numéricos
    if c_pacotes:
        df[c_pacotes] = pd.to_numeric(df[c_pacotes], errors='coerce').fillna(0).astype(int)

    # Strings
    for c in [c_origem, c_trip, c_status, c_doca, c_turno]:
        if c: df[c] = df[c].astype(str).str.strip().fillna('')

    # Filtros
    if c_status:
        df[c_status] = df[c_status].replace({
            'Pendente Recepção': 'pendente recepção',
            'Pendente De Chegada': 'pendente de chegada'
        })
        df = df[~df[c_status].str.lower().str.contains('finalizado', na=False)]

    agora = get_agora_br()
    ini_dia, fim_dia = periodo_dia_filtro(agora)

    em_doca, em_fila, pendentes = [], [], {}
    status_pend = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        status = row[c_status].lower() if c_status else ''
        trip = row[c_trip] if c_trip else 'N/A'
        
        # --- ORIGEM (Lê o valor exato da Coluna C) ---
        origem = row[c_origem] if c_origem else '--'
        # Se quiser pegar só a parte do meio/fim por causa dos underscores, avise.
        # Por enquanto manda a string completa (Ex: FM Hub_SP_Barueri)
        
        doca_val = padronizar_doca(row[c_doca]) if c_doca else '--'
        
        eta_val = row[c_eta] if c_eta else pd.NaT
        cheg_val = row[c_cheg] if c_cheg else pd.NaT
        ent_val = row[c_entrada] if c_entrada else pd.NaT

        # Pendentes
        if status in status_pend and pd.notna(eta_val) and ini_dia <= eta_val <= fim_dia:
            t = row[c_turno] if c_turno else 'T?'
            p_qtd = row[c_pacotes] if c_pacotes else 0
            if t not in pendentes: pendentes[t] = {'lts':0, 'pct':0}
            pendentes[t]['lts'] += 1
            pendentes[t]['pct'] += p_qtd

        # Tempo de Pátio
        minutos = None
        if pd.notna(ent_val):
            minutos = int((agora - ent_val).total_seconds() / 60)

        # Formatação
        eta_str = formatar_data_visual(eta_val)
        cheg_str = formatar_data_visual(cheg_val)
        tempo_str = minutos_para_hhmm(minutos)

        linha = f"- {trip} | Doca: {doca_val} | ETA: {eta_str} | Cheg: {cheg_str} | Tempo: {tempo_str} | {origem}"
        linha_fila = f"- {trip} | ETA: {eta_str} | Cheg: {cheg_str} | Tempo: {tempo_str} | {origem}"

        if 'em doca' in status and minutos is not None:
            em_doca.append((minutos, linha))
        elif 'fila' in status and minutos is not None:
            em_fila.append((minutos, linha_fila))

    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)
    
    msg = []
    if em_doca:
        msg.append(f"🚛 Em Doca: {len(em_doca)} LT(s)\n" + "\n".join([x[1] for x in em_doca]))
    if em_fila:
        msg.append(f"🔴 Em Fila: {len(em_fila)} LT(s)\n" + "\n".join([x[1] for x in em_fila]))

    total_lts = sum(p['lts'] for p in pendentes.values())
    if total_lts > 0:
        total_pct = sum(p['pct'] for p in pendentes.values())
        msg.append(f"⏳ Pendentes: {total_lts} LT(s) ({total_pct} pct)")
        for t, d in ordenar_turnos(pendentes):
            msg.append(f"- {d['lts']} LTs ({d['pct']} pct) no {t}")
    elif not em_doca and not em_fila:
        msg.append("✅ Nenhuma pendência.")

    if msg:
        texto = "Segue as LH´s com mais tempo de Pátio:\n\n" + "\n\n".join(msg)
        enviar_webhook(texto)
    else:
        print("ℹ️ Nada a enviar.")

if __name__ == '__main__':
    try: main()
    except Exception as e: print(f"❌ Erro Crítico: {e}")
