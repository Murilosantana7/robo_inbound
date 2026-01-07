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
        print("ℹ️ Credencial detectada como Base64 e decodificada.")
    except (binascii.Error, ValueError):
        creds_json_str = creds_raw

    try:
        creds_dict = json.loads(creds_json_str)
        return gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
    except Exception as e:
        print(f"❌ Erro ao autenticar: {e}")
        return None

# --- Função de Webhook ---
def enviar_webhook(mensagem_txt):
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url:
        print("❌ Erro: Variável 'SEATALK_WEBHOOK_URL' não definida.")
        return
    
    print("--- CONTEÚDO DA MENSAGEM (PREVIEW) ---")
    print(mensagem_txt[:500] + ("\n... [restante da mensagem] ..." if len(mensagem_txt) > 500 else "")) 
    print("--------------------------------------")

    try:
        payload = {
            "tag": "text",
            "text": { "format": 1, "content": f"```\n{mensagem_txt}\n```" }
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        
        try:
            resp_json = response.json()
            if resp_json.get('code') not in [0, 200]:
                print(f"⚠️ AVISO SEATALK: {resp_json}")
            else:
                print("✅ Mensagem enviada com sucesso!")
        except:
            print("✅ Mensagem enviada (Resposta não-JSON).")
        
    except requests.exceptions.RequestException as err:
        print(f"❌ Erro de conexão/HTTP ao enviar webhook: {err}")
        if hasattr(err, 'response') and err.response is not None:
             print(f"   Detalhe da resposta: {err.response.text}")

# --- Funções Auxiliares ---
def minutos_para_hhmm(minutos):
    sinal = "-" if minutos < 0 else ""
    m = abs(minutos)
    horas = m // 60
    mins = m % 60
    return f"{sinal}{horas:02d}:{mins:02d}h"

def turno_atual(agora_br):
    hora_time = agora_br.time()
    if hora_time >= dt_time(6, 0) and hora_time < dt_time(14, 0): return "T1"
    elif hora_time >= dt_time(14, 0) and hora_time < dt_time(22, 0): return "T2"
    else: return "T3"

def ordenar_turnos(pendentes_por_turno, agora_br):
    ordem_turnos = ['T1', 'T2', 'T3']
    t_atual = turno_atual(agora_br)
    idx = ordem_turnos.index(t_atual)
    nova_ordem = ordem_turnos[idx:] + ordem_turnos[:idx]
    turnos_existentes = {k: v for k, v in pendentes_por_turno.items() if k in nova_ordem}
    return sorted(turnos_existentes.items(), key=lambda x: nova_ordem.index(x[0]))

def periodo_dia_customizado(agora_br):
    hoje = agora_br.date()
    inicio_dia = datetime.combine(hoje, dt_time(6, 0))
    if agora_br < inicio_dia:
        inicio_dia -= timedelta(days=1)
    fim_dia = inicio_dia + timedelta(days=1) - timedelta(seconds=1)
    return inicio_dia, fim_dia

def padronizar_doca(doca_str):
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

# --- Função Principal ---
def main():
    print(f"🔄 Script 'main' iniciado.")
    
    agora_br = datetime.utcnow() - timedelta(hours=3)
    agora_br = agora_br.replace(second=0, microsecond=0)
    print(f"🕒 Horário de Referência (Brasília): {agora_br}")

    cliente = autenticar_e_criar_cliente()
    if not cliente: return

    valores = None
    for i in range(3):
        try:
            planilha = cliente.open_by_key(SPREADSHEET_ID)
            aba = planilha.worksheet(NOME_ABA)
            valores = aba.get('A1:AC8000')
            print("✅ Planilha aberta.")
            break
        except Exception as e:
            print(f"⚠️ Tentativa {i+1} falhou: {e}")
            time.sleep(5)
    
    if not valores:
        enviar_webhook("❌ Falha crítica: Não foi possível ler a planilha após 3 tentativas.")
        return

    # --- CONFIGURAÇÃO DE COLUNAS ---
    COL_TRIP    = 'LH Trip Nnumber'
    COL_ETA     = 'ETA Planejado'
    COL_ORIGEM  = 'station_code'
    COL_CHECKIN = 'Checkin'
    COL_ENTRADA = 'Add to Queue Time'
    COL_PACOTES = 'SUM de Pending Inbound Parcel Qty'
    COL_STATUS  = 'Status'
    COL_TURNO   = 'Turno'
    COL_DOCA    = 'Doca'

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
    
    print("ℹ️ Convertendo colunas de data...")
    if COL_CHECKIN in df.columns:
        df[COL_CHECKIN] = pd.to_datetime(df[COL_CHECKIN], dayfirst=True, errors='coerce')
    else:
        if len(df.columns) > 3: df[COL_CHECKIN] = pd.to_datetime(df.iloc[:, 3], dayfirst=True, errors='coerce')

    if COL_ENTRADA in df.columns:
        df[COL_ENTRADA] = pd.to_datetime(df[COL_ENTRADA], dayfirst=True, errors='coerce')
    else:
         if len(df.columns) > 6: df[COL_ENTRADA] = pd.to_datetime(df.iloc[:, 6], dayfirst=True, errors='coerce')

    if COL_ETA in df.columns:
        df[COL_ETA] = pd.to_datetime(df[COL_ETA], dayfirst=True, errors='coerce')
    
    if COL_PACOTES in df.columns:
        df[COL_PACOTES] = pd.to_numeric(df[COL_PACOTES], errors='coerce').fillna(0).astype(int)

    if COL_STATUS in df.columns:
        df[COL_STATUS] = df[COL_STATUS].astype(str).str.strip()
        df[COL_STATUS] = df[COL_STATUS].replace({'Pendente Recepção': 'pendente recepção', 'Pendente De Chegada': 'pendente de chegada'})
        df = df[~df[COL_STATUS].fillna('').str.lower().str.contains('finalizado')]

    inicio_dia, fim_dia = periodo_dia_customizado(agora_br)
    
    em_doca, em_fila, pendentes_por_turno = [], [], {}
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        trip = str(row.get(COL_TRIP, '???')).strip()
        status = str(row.get(COL_STATUS, '')).strip().lower()
        origem = row.get(COL_ORIGEM, '--')
        if pd.isna(origem) or str(origem).strip() == '': origem = '--'
        
        eta = row.get(COL_ETA)
        if status in pendentes_status and pd.notna(eta) and inicio_dia <= eta <= fim_dia:
            t = str(row.get(COL_TURNO, 'Indef')).strip()
            if t not in pendentes_por_turno: pendentes_por_turno[t] = {'lts': 0, 'pacotes': 0}
            pendentes_por_turno[t]['lts'] += 1
            pendentes_por_turno[t]['pacotes'] += row.get(COL_PACOTES, 0)

        val_checkin = row.get(COL_CHECKIN)
        val_entrada = row.get(COL_ENTRADA)
        data_referencia = val_checkin if pd.notna(val_checkin) else val_entrada
        
        eta_str = eta.strftime('%d/%m %H:%M') if pd.notna(eta) else '--/-- --:--'
        chegada_str = data_referencia.strftime('%d/%m %H:%M') if pd.notna(data_referencia) else '--/-- --:--'
        
        doca_val = row.get(COL_DOCA, '--')
        doca_limpa = padronizar_doca(str(doca_val))

        minutos = -999999
        if pd.notna(data_referencia):
            minutos = int((agora_br - data_referencia).total_seconds() / 60)

        if pd.notna(data_referencia) or status == 'em doca' or 'fila' in status:
            tempo_fmt = minutos_para_hhmm(minutos) if minutos != -999999 else "--:--"
            
            # --- TABELA SEM HÍFEN E ALINHADA ---
            linha_tabela = f"{trip:^14} | {doca_limpa:^8} | {eta_str:^13} | {chegada_str:^13} | {tempo_fmt:^11} | {origem}"
            
            if 'fila' in status:
                em_fila.append((minutos, linha_tabela))
            elif status == 'em doca':
                em_doca.append((minutos, linha_tabela))

    # --- ORDENAÇÃO E LIMITE DE 30 LINHAS ---
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)

    # Aplica o corte para evitar erro de tamanho no Seatalk
    em_doca = em_doca[:30]
    em_fila = em_fila[:30]

    mensagem = []
    
    # Cabeçalho ajustado sem os espaços iniciais
    header_tabela = f"{'LT':^14} | {'Doca':^8} | {'ETA':^13} | {'Chegada':^13} | {'Tempo CD':^11} | Origem"

    if em_doca:
        qtd = len(em_doca)
        texto = "\n".join([x[1] for x in em_doca])
        mensagem.append(f"🚛 Em Doca: {qtd} LT(s)\n{header_tabela}\n{texto}")

    if em_fila:
        qtd = len(em_fila)
        texto = "\n".join([x[1] for x in em_fila])
        mensagem.append(f"🔴 Em Fila: {qtd} LT(s)\n{header_tabela}\n{texto}")

    total_pend = sum(d['lts'] for d in pendentes_por_turno.values())
    if total_pend > 0:
        pcts = sum(d['pacotes'] for d in pendentes_por_turno.values())
        mensagem.append(f"⏳ Pendentes: {total_pend} LTs ({pcts} pct)")
        for t, d in ordenar_turnos(pendentes_por_turno, agora_br):
            mensagem.append(f"- {d['lts']} LTs no {t}")
    elif not em_doca and not em_fila:
        mensagem.append("✅ Nenhuma pendência.")

    if not mensagem:
        print("ℹ️ Nada a enviar.")
        return

    msg_final = "Segue as LH´s com mais tempo de Pátio:\n\n" + "\n\n".join(mensagem)
    print("📤 Enviando mensagem formatada...")
    enviar_webhook(msg_final)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ Erro Fatal: {e}")
        try:
            enviar_webhook(f"Erro Crítico Script: {e}")
        except:
            pass
