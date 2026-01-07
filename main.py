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

# --- AUXILIARES DE FORMATAÇÃO ---
def formatar_saida_data(data_obj):
    """Garante saída visual: 06/01 13:00"""
    if pd.isna(data_obj):
        return "--/-- --:--"
    return data_obj.strftime('%d/%m %H:%M')

def minutos_para_hhmm(minutos):
    """Garante saída visual: 10:36h"""
    if minutos is None: return "--:--h"
    # Filtro para erros de fuso muito grandes (ex: datas de 1900)
    if minutos < -1000: minutos = 0 
    
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
        creds_dict = json.loads(creds_raw)

    try:
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Erro Auth: {e}")
        return None

# --- LÓGICA DE TEMPO ---
def get_agora_br():
    # Hora Brasil (UTC-3)
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
    print(f"🔄 Script Iniciado (Formatos Rígidos).")
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

    # --- MAPEAMENTO E CONVERSÃO RÍGIDA DE COLUNAS ---
    # Coluna B (Index 1) -> dd/mm/yyyy hh:mm
    # Coluna D (Index 3) -> dd/mm/yyyy hh:mm
    # Coluna G (Index 6) -> dd/mm/yyyy hh:mm
    # Coluna I (Index 8) -> hh:mm
    # Coluna O (Index 14)-> dd/mm/yyyy
    
    # Dicionário: {Index: Formato}
    mapa_formatos = {
        1: '%d/%m/%Y %H:%M',  # B: ETA
        3: '%d/%m/%Y %H:%M',  # D: Chegada
        6: '%d/%m/%Y %H:%M',  # G: Add to Queue (Entrada)
        8: '%H:%M',           # I: Apenas hora
        14: '%d/%m/%Y'        # O: Apenas data
    }

    print("🛠️ Aplicando formatos definidos pelo usuário...")
    
    col_nomes = {} # Guarda o nome da coluna para usar na lógica depois

    for idx, formato in mapa_formatos.items():
        try:
            nome_col = df.columns[idx]
            col_nomes[idx] = nome_col # Salva referência
            
            # Força conversão para string e limpa espaços
            df[nome_col] = df[nome_col].astype(str).str.strip()
            
            # Converte usando o formato estrito
            df[nome_col] = pd.to_datetime(df[nome_col], format=formato, errors='coerce')
        except IndexError:
            print(f"⚠️ Aviso: Coluna índice {idx} não existe na planilha.")
        except Exception as e:
            print(f"⚠️ Erro ao processar coluna {idx}: {e}")

    # --- DEFINIÇÃO DE VARIÁVEIS CHAVE ---
    # Usa os índices mapeados acima para garantir que estamos pegando a coluna certa
    # Se der erro de índice, tenta pegar pelo nome antigo como fallback
    try:
        col_eta = df.columns[1]      # B
        col_chegada = df.columns[3]  # D
        col_entrada = df.columns[6]  # G (Add to Queue Time)
        col_pacotes = df.columns[5]  # F (Mantido fixo)
        col_origem = df.columns[28]  # AC (Mantido fixo - se mudou avise)
        col_trip = 'LH Trip Nnumber' # Busca pelo nome pois pode variar posição
        col_status = 'Satus 2.0'     # Busca pelo nome
        col_turno = 'Turno 2'        # Busca pelo nome
        col_doca = 'Doca'            # Busca pelo nome
    except:
        print("❌ Erro crítico na estrutura da planilha.")
        return

    # Limpeza de numéricos
    df[col_pacotes] = pd.to_numeric(df[col_pacotes], errors='coerce').fillna(0).astype(int)
    
    # Limpeza de Strings
    cols_str = [col_status, col_doca, col_turno, col_origem, col_trip]
    for c in cols_str:
        if c in df.columns: df[c] = df[c].astype(str).str.strip().fillna('')

    # Filtros
    if col_status in df.columns:
        df[col_status] = df[col_status].replace({
            'Pendente Recepção': 'pendente recepção', 
            'Pendente De Chegada': 'pendente de chegada'
        })
        df = df[~df[col_status].str.lower().str.contains('finalizado', na=False)]

    agora = get_agora_br()
    inicio_dia, fim_dia = periodo_dia_filtro(agora)

    em_doca, em_fila, pendentes = [], [], {}
    status_pend = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        # Verificações de segurança para colunas que podem não existir
        trip = row[col_trip] if col_trip in df.columns else 'N/A'
        status = row[col_status].lower() if col_status in df.columns else ''
        origem = row[col_origem] if col_origem in df.columns and row[col_origem] else '--'
        eta_val = row[col_eta]
        chegada_val = row[col_chegada]
        
        # Pendentes
        if status in status_pend and pd.notna(eta_val) and inicio_dia <= eta_val <= fim_dia:
            t = row[col_turno] if col_turno in df.columns else 'T?'
            if t not in pendentes: pendentes[t] = {'lts':0, 'pct':0}
            pendentes[t]['lts'] += 1
            pendentes[t]['pct'] += row[col_pacotes]

        # Tempo de Pátio
        entrada = row[col_entrada]
        minutos = None
        if pd.notna(entrada):
            # CÁLCULO: AGORA (BR) - ENTRADA (Coluna G formatada)
            minutos = int((agora - entrada).total_seconds() / 60)

        # Formatação Visual
        eta_fmt = formatar_saida_data(eta_val)
        cheg_fmt = formatar_saida_data(chegada_val)
        tempo_fmt = minutos_para_hhmm(minutos)
        
        doca_val = padronizar_doca(row[col_doca]) if col_doca in df.columns else '--'

        linha = f"- {trip} | Doca: {doca_val} | ETA: {eta_fmt} | Cheg: {cheg_fmt} | Tempo: {tempo_fmt} | {origem}"
        linha_fila = f"- {trip} | ETA: {eta_fmt} | Cheg: {cheg_fmt} | Tempo: {tempo_fmt} | {origem}"

        if status == 'em doca' and minutos is not None:
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
        texto_final = "Segue as LH´s com mais tempo de Pátio:\n\n" + "\n\n".join(msg)
        enviar_webhook(texto_final)
    else:
        print("ℹ️ Nada a enviar.")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ Erro Main: {e}")
