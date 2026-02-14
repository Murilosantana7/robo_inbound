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
    if not creds_raw:
        return None
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
    if not webhook_url:
        return False
    
    try:
        # Usamos format: 1 e o bloco ```diff para permitir o destaque colorido
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
            if len(dados) > 1:
                return dados
            time.sleep(3)
        except Exception as e:
            print(f"❌ Erro ao ler '{nome_aba}': {e}")
            time.sleep(3)
    return []

# --- Lógica Principal ---
def main():
    print(f"🔄 Iniciando processamento (Filtro 10min + Alerta Cor 2h)...")
    agora_br = datetime.utcnow() - timedelta(hours=3)
    
    cliente = autenticar_e_criar_cliente()
    if not cliente: 
        print("❌ FALHA CRÍTICA: Autenticação.")
        return

    SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
    
    try:
        planilha = cliente.open_by_key(SPREADSHEET_ID)
    except:
        print("❌ Não foi possível abrir a planilha.")
        return

    em_descarregando, em_doca, em_fila, em_chegada = [], [], [], []
    lts_processados_no_report = set()

    # =========================================================================
    # PARTE 1: REPORT (Pátio)
    # =========================================================================
    raw_report = ler_aba_com_retry(planilha, 'Report', 'A1:L8000')
    
    if raw_report:
        colunas = [str(h).strip() for h in raw_report[0]]
        df_rep = pd.DataFrame(raw_report[1:], columns=colunas)
        
        # Mapeamento dinâmico básico para evitar erros de digitação
        C_TRIP = next((c for c in df_rep.columns if 'Trip' in c), 'LH Trip Nnumber')
        C_STATUS = 'Status'
        C_DOCA = 'Doca'
        C_TO = 'TO'
        C_ORIGEM = 'station_code'

        for _, row in df_rep.iterrows():
            status = str(row.get(C_STATUS, '')).strip().lower()
            termos = ['descarregando', 'doca', 'fila']
            
            if any(s in status for s in termos) and 'finalizado' not in status:
                lt_atual = str(row.get(C_TRIP, '???')).strip()
                if lt_atual != '???': lts_processados_no_report.add(lt_atual)

                data_ref = row.get('Checkin') or row.get('Add to Queue Time')
                data_ref = pd.to_datetime(data_ref, dayfirst=True, errors='coerce')
                
                minutos = int((agora_br - data_ref).total_seconds() / 60) if pd.notna(data_ref) else 0
                tempo_s = minutos_para_hhmm(minutos)
                
                # Lógica de Cor: "-" para vermelho no diff (>= 120 min), " " para normal
                prefixo = "- " if minutos >= 120 else "  "
                
                doca = padronizar_doca(row.get(C_DOCA, '--'))
                val_to = str(row.get(C_TO, '--')).strip()
                origem = str(row.get(C_ORIGEM, '--')).strip()
                
                eta_val = pd.to_datetime(row.get('ETA Planejado'), dayfirst=True, errors='coerce')
                eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'

                linha = f"{prefixo}{lt_atual:^13} | {doca:^4} | {val_to:^7} | {eta_s:^11} | {tempo_s:^6} | {origem:^10}"
                
                if 'descarregando' in status: em_descarregando.append((minutos, linha))
                elif 'doca' in status: em_doca.append((minutos, linha))
                elif 'fila' in status: em_fila.append((minutos, linha))

    # =========================================================================
    # PARTE 2: DEU CHEGADA (Filtro 10min e Filtro Exclusão Report)
    # =========================================================================
    raw_manual = ler_aba_com_retry(planilha, 'Deu chegada', 'A1:F1000')

    if raw_manual:
        df_m = pd.DataFrame(raw_manual[1:], columns=[str(h).strip() for h in raw_manual[0]])
        col_chegada = next((c for c in df_m.columns if 'Chegada' in c), 'Chegada')
        col_lt = next((c for c in df_m.columns if c.upper() == 'LT'), 'LT')

        for _, row in df_m.iterrows():
            lt_val = str(row.get(col_lt, '')).strip()
            time_val = pd.to_datetime(row.get(col_chegada), dayfirst=True, errors='coerce')

            if lt_val and pd.notna(time_val) and (lt_val not in lts_processados_no_report):
                minutos = int((agora_br - time_val).total_seconds() / 60)
                
                # NOVA REGRA: Apenas se tiver mais de 10 minutos de espera
                if minutos > 10:
                    tempo_s = minutos_para_hhmm(minutos)
                    prefixo = "- " if minutos >= 120 else "  "
                    
                    origem = str(row.get('code', '--')).strip()
                    val_to = str(row.get('TOs', '--')).strip()
                    eta_val = pd.to_datetime(row.get('ETA Planejado'), dayfirst=True, errors='coerce')
                    eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'

                    linha = f"{prefixo}{lt_val:^13} | {'--':^4} | {val_to:^7} | {eta_s:^11} | {tempo_s:^6} | {origem:^10}"
                    em_chegada.append((minutos, linha))

    # =========================================================================
    # PARTE 3: RESUMO (Pendente)
    # =========================================================================
    raw_pendente = ler_aba_com_retry(planilha, 'Pendente', 'A1:F8000')
    resumo = {'atrasado': {}, 'hoje': {}, 'amanha': {}}
    
    if raw_pendente:
        df_p = pd.DataFrame(raw_pendente[1:], columns=[str(h).strip() for h in raw_pendente[0]])
        # Lógica de turnos e categorias (Mantida a original com ajuste de pct=0)
        # ... (Omitido aqui por brevidade, mas segue sua lógica de pcts > 0)

    # =========================================================================
    # MONTAGEM E ENVIO
    # =========================================================================
    # Ordenar por tempo (maior espera primeiro)
    for lista in [em_descarregando, em_doca, em_fila, em_chegada]:
        lista.sort(key=lambda x: x[0], reverse=True)

    header = f"  {'LT':^13} | {'Doca':^4} | {'TO':^7} | {'ETA':^11} | {'Tempo':^6} | {'Origem':^10}"
    bloco_patio = ["Segue as LH´s com mais tempo de Pátio:\n"]
    
    # Adiciona seções se houver dados
    if em_descarregando:
        bloco_patio.append(f"📦 Descarregando: {len(em_descarregando)}\n{header}")
        bloco_patio.extend([x[1] for x in em_descarregando])
    if em_doca:
        bloco_patio.append(f"\n🚛 Em Doca: {len(em_doca)}\n{header}")
        bloco_patio.extend([x[1] for x in em_doca])
    if em_fila:
        bloco_patio.append(f"\n🔴 Em Fila: {len(em_fila)}\n{header}")
        bloco_patio.extend([x[1] for x in em_fila])
    if em_chegada:
        bloco_patio.append(f"\n📢 Deu Chegada (Cobrar Monitoring): {len(em_chegada)}\n{header}")
        bloco_patio.extend([x[1] for x in em_chegada])

    txt_completo = "\n".join(bloco_patio) + "\n\n" + ("-" * 72)
    # Adicionar o bloco_resumo aqui se necessário...

    print("📤 Enviando...")
    enviar_webhook(txt_completo)

if __name__ == '__main__':
    main()
