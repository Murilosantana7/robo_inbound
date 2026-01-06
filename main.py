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

# --- FUNÇÃO DE LEITURA INTELIGENTE (O CORAÇÃO DO SCRIPT) ---
def ler_data_universal(valor):
    """
    Lê datas em qualquer formato (ISO, BR, EUA, Excel) e converte para datetime real.
    Prioriza dia na frente (formato BR) em caso de dúvida (6/1 = 6 de Jan).
    """
    valor_str = str(valor).strip()
    
    # Lista de termos que indicam dado vazio ou inválido
    if not valor_str or valor_str.lower() in ['nat', 'nan', 'none', '', '--', '-', 'null']:
        return pd.NaT

    try:
        # 1. Tenta formato ISO direto (YYYY-MM-DD) - Comum em sistemas
        if '-' in valor_str:
            return pd.to_datetime(valor_str, format='mixed', dayfirst=False)
        
        # 2. Tenta formato Brasileiro (DD/MM/YYYY) - Comum no Excel BR
        # O segredo é o dayfirst=True, que força 06/01 a ser 6 de Janeiro
        return pd.to_datetime(valor_str, dayfirst=True)
    
    except:
        return pd.NaT

# --- AUXILIARES DE FORMATAÇÃO (SAÍDA) ---
def formatar_saida_data(data_obj):
    """Garante a saída estrita: 06/01 13:00"""
    if pd.isna(data_obj):
        return "--/-- --:--"
    return data_obj.strftime('%d/%m %H:%M')

def minutos_para_hhmm(minutos):
    """Garante a saída estrita: 10:36h"""
    # Tratamento para evitar negativos absurdos por erro de fuso
    if minutos < -1000: minutos = 0 # Zera se for erro grosseiro (-3000h)
    
    sinal = "-" if minutos < 0 else ""
    minutos = abs(minutos)
    horas = minutos // 60
    mins = minutos % 60
    return f"{sinal}{horas:02d}:{mins:02d}h"

def padronizar_doca(doca_str):
    if not isinstance(doca_str, str): return "--"
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

# --- WEBHOOK ---
def enviar_webhook(mensagem_txt):
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url:
        print("❌ Erro: 'SEATALK_WEBHOOK_URL' não definida.")
        return

    # Limpeza para evitar quebra do JSON
    mensagem_limpa = str(mensagem_txt).replace('"', "'").replace('\\', '/')
    conteudo_formatado = f"```\n{mensagem_limpa}\n```"

    payload = {
        "tag": "text",
        "text": { 
            "format": 1, 
            "content": conteudo_formatado
        }
    }

    try:
        print("📤 Enviando mensagem...")
        response = requests.post(webhook_url, json=payload, timeout=15)
        if response.status_code != 200 or ("code" in response.text and response.json().get('code') != 0):
            print(f"❌ Erro no envio: {response.text}")
        else:
            print("✅ Sucesso.")
    except Exception as e:
        print(f"❌ Falha crítica webhook: {e}")

# --- AUTENTICAÇÃO ---
def autenticar():
    creds_raw = os.environ.get('GCP_SA_KEY_JSON', '').strip()
    if not creds_raw:
        print("❌ Erro: Variável de credenciais vazia.")
        return None
    try:
        decoded = base64.b64decode(creds_raw, validate=True).decode('utf-8')
        creds_dict = json.loads(decoded)
    except:
        creds_dict = json.loads(creds_raw) # Já era JSON puro

    try:
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Erro Auth: {e}")
        return None

# --- LÓGICA DE TEMPO E TURNOS ---
def get_agora_br():
    # Retorna hora atual BR arredondada (sem segundos)
    return (datetime.utcnow() - timedelta(hours=3)).replace(second=0, microsecond=0)

def turno_atual():
    hora = get_agora_br().time()
    if hora >= dt_time(6, 0) and hora < dt_time(14, 0): return "T1"
    elif hora >= dt_time(14, 0) and hora < dt_time(22, 0): return "T2"
    else: return "T3"

def ordenar_turnos(pendentes):
    ordem = ['T1', 'T2', 'T3']
    idx = ordem.index(turno_atual())
    nova_ordem = ordem[idx:] + ordem[:idx]
    return sorted([i for i in pendentes.items() if i[0] in nova_ordem], key=lambda x: nova_ordem.index(x[0]))

def periodo_dia_filtro(agora_br):
    hoje = agora_br.date()
    inicio = datetime.combine(hoje, dt_time(6, 0))
    if agora_br < inicio: inicio -= timedelta(days=1)
    fim = inicio + timedelta(days=1) - timedelta(seconds=1)
    return inicio, fim

# --- MAIN ---
def main():
    print(f"🔄 Script Universal Iniciado.")
    client = autenticar()
    if not client: return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(NOME_ABA)
        raw_data = sheet.get('A1:AC8000')
        print("✅ Dados baixados.")
    except Exception as e:
        print(f"❌ Erro planilha: {e}")
        enviar_webhook(f"Erro ao ler planilha: {e}")
        return

    if not raw_data: return

    df = pd.DataFrame(raw_data[1:], columns=raw_data[0])
    df.columns = [c.strip() for c in df.columns]

    # Mapeamento de Colunas
    try:
        col_eta = raw_data[0][1].strip()      # B
        col_chegada = raw_data[0][3].strip()  # D
        col_pacotes = raw_data[0][5].strip()  # F
        col_origem = raw_data[0][28].strip()  # AC
    except:
        print("❌ Erro estrutura colunas.")
        return

    # Normalização de Nomes
    if col_eta != 'ETA Planejado': df.rename(columns={col_eta: 'ETA Planejado'}, inplace=True)

    # --- APLICAÇÃO DA LEITURA UNIVERSAL ---
    print("🛠️ Convertendo datas mistas...")
    df['Add to Queue Time'] = df['Add to Queue Time'].apply(ler_data_universal)
    df['ETA Planejado'] = df['ETA Planejado'].apply(ler_data_universal)
    df[col_chegada] = df[col_chegada].apply(ler_data_universal)
    # --------------------------------------

    df[col_pacotes] = pd.to_numeric(df[col_pacotes], errors='coerce').fillna(0).astype(int)
    
    # Limpeza de Strings
    cols_str = ['Satus 2.0', 'Doca', 'Turno 2', col_origem, 'LH Trip Nnumber']
    for c in cols_str:
        if c in df.columns: df[c] = df[c].astype(str).str.strip().fillna('')

    # Filtros
    df['Satus 2.0'] = df['Satus 2.0'].replace({
        'Pendente Recepção': 'pendente recepção', 
        'Pendente De Chegada': 'pendente de chegada'
    })
    df = df[~df['Satus 2.0'].str.lower().str.contains('finalizado', na=False)]

    # Referência de Tempo (Brasil)
    agora = get_agora_br()
    inicio_dia, fim_dia = periodo_dia_filtro(agora)

    em_doca, em_fila, pendentes = [], [], {}
    status_pend = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        trip = row['LH Trip Nnumber']
        status = row['Satus 2.0'].lower()
        origem = row[col_origem] if row[col_origem] else '--'
        eta_val = row['ETA Planejado']
        chegada_val = row[col_chegada]
        
        # Lógica Pendentes
        if status in status_pend and pd.notna(eta_val) and inicio_dia <= eta_val <= fim_dia:
            t = row['Turno 2']
            if t not in pendentes: pendentes[t] = {'lts':0, 'pct':0}
            pendentes[t]['lts'] += 1
            pendentes[t]['pct'] += row[col_pacotes]

        # Lógica Tempo de Pátio
        entrada = row['Add to Queue Time']
        minutos = None
        if pd.notna(entrada):
            # Como convertemos tudo para datetime sem fuso (naive) e 'agora' também é naive BR,
            # a subtração funciona direto.
            minutos = int((agora - entrada).total_seconds() / 60)

        # --- MONTAGEM DAS STRINGS (AQUI APLICAMOS SEU PEDIDO DE FORMATO) ---
        eta_fmt = formatar_saida_data(eta_val)      # Sai como 06/01 13:00
        cheg_fmt = formatar_saida_data(chegada_val) # Sai como 06/01 13:00
        tempo_fmt = minutos_para_hhmm(minutos) if minutos is not None else "--:--h" # Sai como 10:36h

        linha = f"- {trip} | Doca: {padronizar_doca(row['Doca'])} | ETA: {eta_fmt} | Cheg: {cheg_fmt} | Tempo: {tempo_fmt} | {origem}"
        linha_fila = f"- {trip} | ETA: {eta_fmt} | Cheg: {cheg_fmt} | Tempo: {tempo_fmt} | {origem}"
        # ------------------------------------------------------------------

        if status == 'em doca' and minutos is not None:
            em_doca.append((minutos, linha))
        elif 'fila' in status and minutos is not None:
            em_fila.append((minutos, linha_fila))

    # Ordenação e Output
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
        texto_final = "Segue as LH´s com mais tempo de Pátio:\n\n" + "\n\n".join(msg)
        enviar_webhook(texto_final)
    else:
        print("ℹ️ Nada a enviar.")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ Erro Main: {e}")
