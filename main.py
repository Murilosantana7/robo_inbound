# -*- coding: utf-8 -*-
import pandas as pd
import gspread
import requests
from datetime import datetime, timedelta
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
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url: return False
    try:
        # O uso do ```diff permite que linhas iniciadas com '-' fiquem vermelhas
        payload = {
            "tag": "text",
            "text": { "format": 1, "content": f"```diff\n{mensagem_txt}\n```" }
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
    match = re.search(r'(\d+)$', str(doca_str))
    return match.group(1) if match else "--"

def ler_aba_com_retry(planilha, nome_aba, range_celulas):
    for tentativa in range(3):
        try:
            dados = planilha.worksheet(nome_aba).get(range_celulas)
            if len(dados) > 1: return dados
            time.sleep(3)
        except:
            time.sleep(3)
    return []

# --- Lógica Principal ---
def main():
    agora_br = datetime.utcnow() - timedelta(hours=3)
    cliente = autenticar_e_criar_cliente()
    if not cliente: return

    SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
    try:
        planilha = cliente.open_by_key(SPREADSHEET_ID)
    except:
        return

    em_descarregando, em_doca, em_fila, em_chegada = [], [], [], []
    lts_processados_no_report = set()

    # 1. PROCESSAR REPORT (PÁTIO)
    raw_report = ler_aba_com_retry(planilha, 'Report', 'A1:L8000')
    if raw_report:
        df_rep = pd.DataFrame(raw_report[1:], columns=[str(h).strip() for h in raw_report[0]])
        for _, row in df_rep.iterrows():
            status = str(row.get('Status', '')).strip().lower()
            if any(s in status for s in ['descarregando', 'doca', 'fila']) and 'finalizado' not in status:
                lt_atual = str(row.get('LH Trip Nnumber', '???')).strip()
                lts_processados_no_report.add(lt_atual)

                data_ref = row.get('Checkin') or row.get('Add to Queue Time')
                data_ref = pd.to_datetime(data_ref, dayfirst=True, errors='coerce')
                
                minutos = int((agora_br - data_ref).total_seconds() / 60) if pd.notna(data_ref) else 0
                tempo_s = minutos_para_hhmm(minutos)
                
                # Formatação idêntica ao seu print
                prefixo = "- " if minutos >= 120 else "  "
                doca = padronizar_doca(row.get('Doca', '--'))
                val_to = str(row.get('TO', '--')).strip()
                origem = str(row.get('station_code', '--')).strip()
                eta_val = pd.to_datetime(row.get('ETA Planejado'), dayfirst=True, errors='coerce')
                eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'

                linha = f"{prefixo}• {lt_atual} | {doca} | {val_to} | {eta_s} | {tempo_s} | {origem}"
                
                if 'descarregando' in status: em_descarregando.append((minutos, linha))
                elif 'doca' in status: em_doca.append((minutos, linha))
                elif 'fila' in status: em_fila.append((minutos, linha))

    # 2. PROCESSAR DEU CHEGADA (COM FILTRO 10 MINUTOS)
    raw_manual = ler_aba_com_retry(planilha, 'Deu chegada', 'A1:F1000')
    if raw_manual:
        df_m = pd.DataFrame(raw_manual[1:], columns=[str(h).strip() for h in raw_manual[0]])
        for _, row in df_m.iterrows():
            lt = str(row.get('LT', '')).strip()
            chegada = pd.to_datetime(row.get('Chegada'), dayfirst=True, errors='coerce')
            if lt and pd.notna(chegada) and (lt not in lts_processados_no_report):
                minutos = int((agora_br - chegada).total_seconds() / 60)
                
                if minutos > 10: # Só mostra se tiver mais de 10 min de espera
                    tempo_s = minutos_para_hhmm(minutos)
                    prefixo = "- " if minutos >= 120 else "  "
                    origem = str(row.get('code', '--')).strip()
                    val_to = str(row.get('TOs', '--')).strip()
                    eta_val = pd.to_datetime(row.get('ETA Planejado'), dayfirst=True, errors='coerce')
                    eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'
                    
                    linha = f"{prefixo}• {lt} | -- | {val_to} | {eta_s} | {tempo_s} | {origem}"
                    em_chegada.append((minutos, linha))

    # 3. MONTAGEM FINAL
    header = "      LT | Doca | TO | ETA | Tempo | Origem"
    bloco = ["Segue as LH´s com mais tempo de Pátio:\n"]

    secoes = [
        ("📦 Descarregando", em_descarregando),
        ("🚛 Em Doca", em_doca),
        ("🔴 Em Fila", em_fila),
        ("📢 Deu Chegada (Cobrar Monitoring)", em_chegada)
    ]

    for titulo, lista in secoes:
        if lista:
            lista.sort(key=lambda x: x[0], reverse=True)
            bloco.append(f"{titulo}: {len(lista)}")
            bloco.append(header)
            bloco.extend([x[1] for x in lista])
            bloco.append("")

    enviar_webhook("\n".join(bloco))

if __name__ == '__main__':
    main()
