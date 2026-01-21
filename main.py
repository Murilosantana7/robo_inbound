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

# --- Configurações e Autenticação ---
def autenticar_e_criar_cliente():
    creds_raw = os.environ.get('GCP_SA_KEY_JSON', '').strip()
    if not creds_raw: return None
    try:
        creds_json_str = base64.b64decode(creds_raw, validate=True).decode('utf-8')
    except:
        creds_json_str = creds_raw
    try:
        return gspread.service_account_from_dict(json.loads(creds_json_str), scopes=['https://www.googleapis.com/auth/spreadsheets'])
    except:
        return None

def enviar_webhook(mensagem_txt):
    """
    Tenta enviar a mensagem. Retorna True se sucesso, False se falhar (ex: muito longa).
    """
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url: return False
    
    try:
        payload = {
            "tag": "text",
            "text": { 
                "format": 1, 
                "content": f"```\n{mensagem_txt}\n```" 
            }
        }
        response = requests.post(webhook_url, json=payload)
        return response.status_code == 200
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return False

# --- Funções de Apoio ---
def minutos_para_hhmm(minutos):
    sinal = "-" if minutos < 0 else ""
    m = abs(minutos)
    return f"{sinal}{m // 60:02d}:{m % 60:02d}"

def padronizar_doca(doca_str):
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

# --- Lógica Principal ---
def main():
    print(f"🔄 Iniciando processamento...")
    agora_br = datetime.utcnow() - timedelta(hours=3)
    
    cliente = autenticar_e_criar_cliente()
    if not cliente: 
        print("❌ FALHA CRÍTICA: Não foi possível autenticar. Verifique a variável GCP_SA_KEY_JSON.")
        return

    # Leitura da Planilha
    SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
    try:
        planilha = cliente.open_by_key(SPREADSHEET_ID)
        valores = planilha.worksheet('Tabela dinâmica 2').get('A1:AC8000')
    except Exception as e:
        print(f"❌ Erro leitura: {e}")
        return

    df = pd.DataFrame(valores[1:], columns=[str(h).strip() for h in valores[0]])
    
    # Colunas
    # NOTA: Verifique se na sua planilha o cabeçalho é 'LH Trip Number' mesmo
    COL_TRIP    = 'LH Trip Number' 
    COL_ETA     = 'ETA Planejado'
    COL_ORIGEM  = 'station_code'
    COL_CHECKIN = 'Checkin'
    COL_ENTRADA = 'Add to Queue Time'
    COL_PACOTES = 'SUM de total_orders' 
    COL_STATUS  = 'Status'
    COL_TURNO   = 'Turno'
    COL_DOCA    = 'Doca'
    COL_CUTOFF  = 'Cutoff'

    # Tratamento
    # Garante que a coluna de pacotes seja numérica
    df[COL_PACOTES] = pd.to_numeric(df[COL_PACOTES], errors='coerce').fillna(0).astype(int)
    df = df[df[COL_PACOTES] > 0] 

    for col in [COL_CHECKIN, COL_ENTRADA, COL_ETA, COL_CUTOFF]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    # Turnos e Datas
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

    for _, row in df.iterrows():
        status = str(row.get(COL_STATUS, '')).strip().lower()
        status = status.replace('pendente recepção', 'pendente de chegada')
        
        # Resumos
        if 'pendente' in status:
            t = str(row.get(COL_TURNO, 'Indef')).strip()
            cutoff = row.get(COL_CUTOFF)
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
                resumo[categoria][t]['pacotes'] += row[COL_PACOTES]

        # Tabela Pátio
        data_ref = row[COL_CHECKIN] if pd.notna(row[COL_CHECKIN]) else row[COL_ENTRADA]
        if pd.notna(data_ref) or status == 'em doca' or 'fila' in status:
            if 'finalizado' in status: continue
            
            trip = str(row.get(COL_TRIP, '???')).strip()
            doca = padronizar_doca(str(row.get(COL_DOCA, '--')))
            eta_s = row[COL_ETA].strftime('%d/%m %H:%M') if pd.notna(row[COL_ETA]) else '--/-- --:--'
            che_s = data_ref.strftime('%d/%m %H:%M') if pd.notna(data_ref) else '--/-- --:--'
            minutos = int((agora_br - data_ref).total_seconds() / 60) if pd.notna(data_ref) else -999999
            tempo = minutos_para_hhmm(minutos)
            
            linha = f"{trip:^13} | {doca:^4} | {eta_s:^11} | {che_s:^11} | {tempo:^6} | {str(row.get(COL_ORIGEM, '--'))}"
            if 'fila' in status: em_fila.append((minutos, linha))
            elif status == 'em doca': em_doca.append((minutos, linha))

    # --- Montagem Visual Otimizada ---
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)
    header = f"{'LT':^13} | {'Doca':^4} | {'ETA':^11} | {'Chegada':^11} | {'Tempo':^6} | Origem"
    
    # 1. Parte das Tabelas
    bloco_patio = ["Segue as LH´s com mais tempo de Pátio:\n"]
    if em_doca:
        bloco_patio.append(f"🚛 Em Doca: {len(em_doca)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_doca])
    if em_fila:
        bloco_patio.append(f"\n🔴 Em Fila: {len(em_fila)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_fila])

    # 2. Parte dos Resumos
    bloco_resumo = []
    str_amanha = op_date_amanha.strftime('%d/%m/%Y')
    titulos = {'atrasado': '⚠️ Atrasados', 'hoje': '📅 Hoje', 'amanha': f'🌅 Amanhã {str_amanha}'}
    
    for cat in ['atrasado', 'hoje', 'amanha']:
        if resumo[cat]:
            total_lts = sum(d['lts'] for d in resumo[cat].values())
            total_pct = sum(d['pacotes'] for d in resumo[cat].values())
            
            bloco_resumo.append(f"{titulos[cat]}: {total_lts} LTs ({total_pct} pct)")
            
            for t in ['T1', 'T2', 'T3']:
                if t in resumo[cat]:
                    bloco_resumo.append(f"   - {t}: {resumo[cat][t]['lts']} LTs ({resumo[cat][t]['pacotes']} pct)")
            
            # --- MELHORIA VISUAL ---
            # Adiciona linha em branco entre as categorias
            bloco_resumo.append("") 

    # --- Estratégia de Envio com Divisória ---
    txt_patio = "\n".join(bloco_patio)
    txt_resumo = "\n".join(bloco_resumo)
    
    # 72 traços correspondem aproximadamente à largura da tabela no celular
    linha_divisoria = "\n" + ("-" * 72) + "\n\n"
    
    txt_completo = txt_patio + linha_divisoria + txt_resumo

    print("📤 Tentando envio único formatado...")
    if not enviar_webhook(txt_completo):
        print("✂️ Mensagem longa demais. Dividindo...")
        enviar_webhook(txt_patio)
        time.sleep(1.5)
        if txt_resumo:
            enviar_webhook(txt_resumo)
    else:
        print("✅ Enviado com sucesso.")

if __name__ == '__main__':
    main()
