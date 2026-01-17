# -*- coding: utf-8 -*-
import pandas as pd
import gspread
import requests
from datetime import datetime, timedelta, time as dt_time
import re
import time
import os
import json
import base64
import binascii

# --- Constantes do Script ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
NOME_ABA = 'Tabela dinâmica 2'

# --- Função de Autenticação ---
def autenticar_e_criar_cliente():
    creds_raw = os.environ.get('GCP_SA_KEY_JSON', '').strip()
    if not creds_raw:
        print("❌ Erro: Variável 'GCP_SA_KEY_JSON' vazia.")
        return None
    try:
        decoded_bytes = base64.b64decode(creds_raw, validate=True)
        creds_json_str = decoded_bytes.decode('utf-8')
    except (binascii.Error, ValueError):
        creds_json_str = creds_raw

    try:
        creds_dict = json.loads(creds_json_str)
        return gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
    except Exception as e:
        print(f"❌ Erro ao autenticar: {e}")
        return None

# --- Função de Webhook (CORRIGIDA PARA MANTER ALINHAMENTO) ---
def enviar_webhook(mensagem_txt):
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url:
        print("❌ Erro: Variável 'SEATALK_WEBHOOK_URL' não definida.")
        return
    
    # O segredo para manter o alinhamento limpo é o uso das 3 crases (bloco de código)
    # format: 1 indica que o SeaTalk deve processar o Markdown
    try:
        payload = {
            "tag": "text",
            "text": { 
                "format": 1, 
                "content": f"```\n{mensagem_txt}\n```" 
            }
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        time.sleep(1.5) # Delay para garantir a ordem das mensagens no chat
    except Exception as e:
        print(f"❌ Erro ao enviar webhook: {e}")

# --- Funções Auxiliares ---
def minutos_para_hhmm(minutos):
    sinal = "-" if minutos < 0 else ""
    m = abs(minutos)
    horas = m // 60
    mins = m % 60
    return f"{sinal}{horas:02d}:{mins:02d}"

def padronizar_doca(doca_str):
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

# --- Função Principal ---
def main():
    print(f"🔄 Script iniciado.")
    agora_br = datetime.utcnow() - timedelta(hours=3)
    agora_br = agora_br.replace(second=0, microsecond=0)

    cliente = autenticar_e_criar_cliente()
    if not cliente: return

    valores = None
    for i in range(3):
        try:
            planilha = cliente.open_by_key(SPREADSHEET_ID)
            aba = planilha.worksheet(NOME_ABA)
            valores = aba.get('A1:AC8000')
            break
        except Exception as e:
            time.sleep(5)
    
    if not valores: return

    headers_originais = [str(h).strip() for h in valores[0]]
    headers_unicos = []
    seen = {}
    for h in headers_originais:
        if h in seen:
            seen[h] += 1
            headers_unicos.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            headers_unicos.append(h)

    df = pd.DataFrame(valores[1:], columns=headers_unicos)
    
    # Mapeamento de Colunas
    COL_TRIP    = 'LH Trip Nnumber'
    COL_ETA     = 'ETA Planejado'
    COL_ORIGEM  = 'station_code'
    COL_CHECKIN = 'Checkin'
    COL_ENTRADA = 'Add to Queue Time'
    COL_PACOTES = 'SUM de total_orders' 
    COL_STATUS  = 'Status'
    COL_TURNO   = 'Turno'
    COL_DOCA    = 'Doca'
    COL_CUTOFF  = 'Cutoff'

    # Conversões
    if COL_CHECKIN in df.columns: df[COL_CHECKIN] = pd.to_datetime(df[COL_CHECKIN], dayfirst=True, errors='coerce')
    if COL_ENTRADA in df.columns: df[COL_ENTRADA] = pd.to_datetime(df[COL_ENTRADA], dayfirst=True, errors='coerce')
    if COL_ETA in df.columns: df[COL_ETA] = pd.to_datetime(df[COL_ETA], dayfirst=True, errors='coerce')
    if COL_CUTOFF in df.columns: df[COL_CUTOFF] = pd.to_datetime(df[COL_CUTOFF], dayfirst=True, errors='coerce')
    if COL_PACOTES in df.columns: df[COL_PACOTES] = pd.to_numeric(df[COL_PACOTES], errors='coerce').fillna(0).astype(int)

    if COL_STATUS in df.columns:
        df[COL_STATUS] = df[COL_STATUS].astype(str).str.strip().replace({'Pendente Recepção': 'pendente recepção', 'Pendente De Chegada': 'pendente de chegada'})
        df = df[~df[COL_STATUS].fillna('').str.lower().str.contains('finalizado')]

    # Definição de data e turno operacional
    if agora_br.time() < dt_time(6, 0): op_date_hoje = agora_br.date() - timedelta(days=1)
    else: op_date_hoje = agora_br.date()
    op_date_amanha = op_date_hoje + timedelta(days=1)

    hora_atual = agora_br.time()
    turno_atual_str = "T3"
    if dt_time(6, 0) <= hora_atual < dt_time(14, 0): turno_atual_str = "T1"
    elif dt_time(14, 0) <= hora_atual < dt_time(22, 0): turno_atual_str = "T2"
    mapa_turnos = {'T1': 1, 'T2': 2, 'T3': 3}

    em_doca, em_fila = [], []
    resumo = {'atrasado': {}, 'hoje': {}, 'amanha': {}}
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        status = str(row.get(COL_STATUS, '')).strip().lower()
        qtd_pacotes = row.get(COL_PACOTES, 0)
        if qtd_pacotes <= 0: continue

        val_checkin = row.get(COL_CHECKIN)
        cutoff = row.get(COL_CUTOFF)

        # Resumo Gerencial
        if status in pendentes_status:
            t = str(row.get(COL_TURNO, 'Indef')).strip()
            categoria = None
            if pd.notna(cutoff):
                d_cutoff = cutoff.date()
                if d_cutoff < op_date_hoje: categoria = 'atrasado'
                elif d_cutoff == op_date_hoje:
                    categoria = 'atrasado' if mapa_turnos.get(t, 99) < mapa_turnos.get(turno_atual_str, 0) else 'hoje'
                elif d_cutoff == op_date_amanha: categoria = 'amanha'
            
            if categoria:
                if t not in resumo[categoria]: resumo[categoria][t] = {'lts': 0, 'pacotes': 0}
                resumo[categoria][t]['lts'] += 1
                resumo[categoria][t]['pacotes'] += qtd_pacotes

        # Detalhamento de Pátio
        val_entrada = row.get(COL_ENTRADA)
        data_ref = val_checkin if pd.notna(val_checkin) else val_entrada
        if pd.notna(data_ref) or status == 'em doca' or 'fila' in status:
            trip = str(row.get(COL_TRIP, '???')).strip()
            doca = padronizar_doca(str(row.get(COL_DOCA, '--')))
            eta_s = row[COL_ETA].strftime('%d/%m %H:%M') if pd.notna(row[COL_ETA]) else '--/-- --:--'
            che_s = data_ref.strftime('%d/%m %H:%M') if pd.notna(data_ref) else '--/-- --:--'
            minutos = int((agora_br - data_ref).total_seconds() / 60) if pd.notna(data_ref) else -999999
            tempo = minutos_para_hhmm(minutos) if minutos != -999999 else "--:--"
            
            linha = f"{trip:^13} | {doca:^4} | {eta_s:^11} | {che_s:^11} | {tempo:^6} | {str(row.get(COL_ORIGEM, '--'))}"
            if 'fila' in status: em_fila.append((minutos, linha))
            elif status == 'em doca': em_doca.append((minutos, linha))

    # --- MONTAGEM E ENVIO ---
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)
    header = f"{'LT':^13} | {'Doca':^4} | {'ETA':^11} | {'Chegada':^11} | {'Tempo':^6} | Origem"
    
    # PARTE 1: Cabeçalho + Tabelas (Doca e Fila)
    msg_parte1 = ["Segue as LH´s com mais tempo de Pátio:\n"]
    if em_doca:
        msg_parte1.append(f"🚛 Em Doca: {len(em_doca)} LT(s)\n{header}")
        msg_parte1.extend([x[1] for x in em_doca])
    if em_fila:
        msg_parte1.append(f"\n🔴 Em Fila: {len(em_fila)} LT(s)\n{header}")
        msg_parte1.extend([x[1] for x in em_fila])

    # PARTE 2: Resumos (Atrasados, Hoje, Amanhã)
    msg_parte2 = []
    str_amanha = op_date_amanha.strftime('%d/%m/%Y')
    titulos = {'atrasado': '⚠️ Atrasados', 'hoje': '📅 Hoje', 'amanha': f'🌅 Amanhã {str_amanha}'}
    
    for cat in ['atrasado', 'hoje', 'amanha']:
        if resumo[cat]:
            total_lts = sum(d['lts'] for d in resumo[cat].values())
            total_pct = sum(d['pacotes'] for d in resumo[cat].values())
            msg_parte2.append(f"{titulos[cat]}: {total_lts} LTs ({total_pct} pct)")
            for t in ['T1', 'T2', 'T3']:
                if t in resumo[cat]:
                    d = resumo[cat][t]
                    msg_parte2.append(f"   - {t}: {d['lts']} LTs ({d['pacotes']} pct)")

    # Disparo em duas bolhas para garantir a quebra no local certo e o alinhamento
    if len(msg_parte1) > 1:
        enviar_webhook("\n".join(msg_parte1))
    
    if msg_parte2:
        enviar_webhook("\n".join(msg_parte2))

if __name__ == '__main__':
    main()
