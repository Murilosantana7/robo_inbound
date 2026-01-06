# -*- coding: utf-8 -*-
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials # <--- IMPORTAÇÃO NOVA E CRÍTICA
from datetime import datetime, timedelta, time as dt_time
import re
import time
import os
import json
import base64
import binascii

# --- Constantes do Script ---
# Adicionado o escopo 'drive' para garantir que o token de acesso seja gerado corretamente
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
SPREADSHEET_ID = '1TfzqJZFD3yPNCAXAiLyEw876qjOlitae0pP9TTqNCPI'
NOME_ABA = 'Tabela dinâmica 2'

# --- FUNÇÃO DE WEBHOOK (BLINDADA) ---
def enviar_webhook(mensagem_txt):
    """
    Envia a mensagem para o Seatalk com formatação de código.
    Troca aspas duplas por simples para evitar erro de JSON.
    """
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url:
        print("❌ Erro: Variável 'SEATALK_WEBHOOK_URL' não definida.")
        return

    # Limpeza de Segurança
    mensagem_limpa = str(mensagem_txt).replace('"', "'").replace('\\', '/')
    
    # Montagem com Visual (Bloco de Código)
    conteudo_formatado = f"```\n{mensagem_limpa}\n```"

    payload = {
        "tag": "text",
        "text": { 
            "format": 1, 
            "content": conteudo_formatado
        }
    }

    try:
        print("📤 Enviando mensagem para o Seatalk...")
        response = requests.post(webhook_url, json=payload, timeout=15)
        
        if response.status_code != 200 or ("code" in response.text and response.json().get('code') != 0):
            print(f"❌ Erro no envio: {response.text}")
        else:
            print("✅ Mensagem enviada com sucesso.")

    except Exception as e:
        print(f"❌ Falha crítica ao enviar webhook: {e}")

# --- Função de Autenticação (CORRIGIDA) ---
def autenticar_e_criar_cliente():
    creds_raw = os.environ.get('GCP_SA_KEY_JSON', '').strip()
    if not creds_raw:
        print("❌ Erro: Variável 'GCP_SA_KEY_JSON' vazia.")
        return None

    # Decodificação Base64
    try:
        decoded_bytes = base64.b64decode(creds_raw, validate=True)
        creds_json_str = decoded_bytes.decode('utf-8')
        # print("ℹ️ Credencial Base64 decodificada.")
    except (binascii.Error, ValueError):
        creds_json_str = creds_raw

    try:
        creds_dict = json.loads(creds_json_str)
        
        # --- CORREÇÃO DO ERRO 'NO ACCESS TOKEN' ---
        # Criamos a credencial manualmente com a lib oficial do Google
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        cliente = gspread.authorize(creds)
        print("✅ Cliente autenticado com sucesso (google.oauth2).")
        return cliente
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
        return None

# --- Funções Auxiliares ---
def minutos_para_hhmm(minutos):
    horas = minutos // 60
    mins = minutos % 60
    return f"{horas:02d}:{mins:02d}h"

def turno_atual():
    # UTC - 3h para simular Horário de Brasília
    agora_br = datetime.utcnow() - timedelta(hours=3)
    hora_atual = agora_br.time()
    
    if hora_atual >= dt_time(6, 0) and hora_atual < dt_time(14, 0): return "T1"
    elif hora_atual >= dt_time(14, 0) and hora_atual < dt_time(22, 0): return "T2"
    else: return "T3"

def ordenar_turnos(pendentes_por_turno):
    ordem_turnos = ['T1', 'T2', 'T3']
    t_atual = turno_atual()
    idx = ordem_turnos.index(t_atual)
    nova_ordem = ordem_turnos[idx:] + ordem_turnos[:idx]
    
    turnos_existentes = {k: v for k, v in pendentes_por_turno.items() if k in nova_ordem}
    return sorted(turnos_existentes.items(), key=lambda x: nova_ordem.index(x[0]))

def periodo_dia_customizado(agora_utc):
    hoje = agora_utc.date()
    inicio_dia = datetime.combine(hoje, dt_time(6, 0))
    if agora_utc < inicio_dia:
        inicio_dia -= timedelta(days=1)
    fim_dia = inicio_dia + timedelta(days=1) - timedelta(seconds=1)
    return inicio_dia, fim_dia

def padronizar_doca(doca_str):
    if not isinstance(doca_str, str): return "--"
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

# --- MAIN ---
def main():
    print(f"🔄 Script de Monitoramento Iniciado.")
    cliente = autenticar_e_criar_cliente()
    if not cliente: return

    try:
        planilha = cliente.open_by_key(SPREADSHEET_ID)
        aba = planilha.worksheet(NOME_ABA)
        valores = aba.get('A1:AC8000') 
        print("✅ Dados baixados da planilha.")
    except Exception as e:
        msg_erro = f"Erro crítico ao ler planilha: {e}"
        print(f"❌ {msg_erro}")
        enviar_webhook(msg_erro)
        return

    if not valores:
        print("❌ A planilha retornou vazia.")
        return
    
    df = pd.DataFrame(valores[1:], columns=valores[0])
    df.columns = [col.strip() for col in df.columns]

    # Mapeamento Dinâmico de Colunas
    try:
        header_eta = valores[0][1].strip() # Coluna B
        header_origem = valores[0][28].strip() # Coluna AC
        header_chegada = valores[0][3].strip() # Coluna D
        header_pacotes = valores[0][5].strip() # Coluna F
    except IndexError:
        print("❌ Erro: Estrutura da planilha mudou (colunas insuficientes).")
        return

    # Normalização
    if header_eta != 'ETA Planejado':
        df.rename(columns={header_eta: 'ETA Planejado'}, inplace=True)

    # Conversão de Tipos
    df['Add to Queue Time'] = pd.to_datetime(df['Add to Queue Time'], errors='coerce')
    df['ETA Planejado'] = pd.to_datetime(df['ETA Planejado'], format='%d/%m/%Y %H:%M', errors='coerce')
    df[header_chegada] = pd.to_datetime(df[header_chegada], format='%d/%m/%Y %H:%M', errors='coerce')
    df[header_pacotes] = pd.to_numeric(df[header_pacotes], errors='coerce').fillna(0).astype(int)
    
    # Sanitização de Strings
    cols_string = ['Satus 2.0', 'Doca', 'Turno 2', header_origem, 'LH Trip Nnumber']
    for col in cols_string:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().fillna('')

    # Filtros
    df['Satus 2.0'] = df['Satus 2.0'].replace({'Pendente Recepção': 'pendente recepção', 'Pendente De Chegada': 'pendente de chegada'})
    df = df[~df['Satus 2.0'].str.lower().str.contains('finalizado', na=False)]

    # Definição de Tempo (UTC para cálculo de data, -3h visual é feito na função turno)
    agora_utc = datetime.utcnow().replace(second=0, microsecond=0)
    inicio_dia, fim_dia = periodo_dia_customizado(agora_utc)

    em_doca, em_fila, pendentes_por_turno = [], [], {}
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        trip = row['LH Trip Nnumber']
        status = row['Satus 2.0'].lower()
        origem = row[header_origem] if row[header_origem] else '--'
        pacotes = row[header_pacotes]
        eta_pendente = row['ETA Planejado']
        turno = row['Turno 2']

        # Lógica Pendentes
        if status in pendentes_status and pd.notna(eta_pendente) and inicio_dia <= eta_pendente <= fim_dia:
            if turno not in pendentes_por_turno:
                pendentes_por_turno[turno] = {'lts': 0, 'pacotes': 0}
            pendentes_por_turno[turno]['lts'] += 1
            pendentes_por_turno[turno]['pacotes'] += pacotes 
            
        # Lógica Doca/Fila
        entrada_cd = row['Add to Queue Time']
        doca = row['Doca'] if row['Doca'] else '--'
        
        eta_str = row['ETA Planejado'].strftime('%d/%m %H:%M') if pd.notna(row['ETA Planejado']) else '--/-- --:--'
        chegada_str = row[header_chegada].strftime('%d/%m %H:%M') if pd.notna(row[header_chegada]) else '--/-- --:--'
        
        minutos = None
        if pd.notna(entrada_cd):
            minutos = int((agora_utc - entrada_cd).total_seconds() / 60)

        if status == 'em doca' and minutos is not None:
            msg_doca = f"- {trip} | Doca: {padronizar_doca(doca)} | ETA: {eta_str} | Cheg: {chegada_str} | Tempo: {minutos_para_hhmm(minutos)} | {origem}"
            em_doca.append((minutos, msg_doca))
        elif 'fila' in status and minutos is not None:
            msg_fila = f"- {trip} | ETA: {eta_str} | Cheg: {chegada_str} | Tempo: {minutos_para_hhmm(minutos)} | {origem}"
            em_fila.append((minutos, msg_fila))

    # Ordenação e Montagem
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)
    mensagem = []

    if em_doca:
        mensagem.append(f"🚛 Em Doca: {len(em_doca)} LT(s)\n" + "\n".join([x[1] for x in em_doca]))
    if em_fila:
        mensagem.append(f"🔴 Em Fila: {len(em_fila)} LT(s)\n" + "\n".join([x[1] for x in em_fila]))

    total_lts = sum(d['lts'] for d in pendentes_por_turno.values())
    total_pkgs = sum(d['pacotes'] for d in pendentes_por_turno.values())

    if total_lts > 0:
        mensagem.append(f"⏳ Pendentes: {total_lts} LT(s) ({total_pkgs} pct)")
        for turno, dados in ordenar_turnos(pendentes_por_turno):
            mensagem.append(f"- {dados['lts']} LTs ({dados['pacotes']} pct) no {turno}")
    elif not em_doca and not em_fila:
        mensagem.append("✅ Nenhuma pendência no momento.")

    if not mensagem:
        print("ℹ️ Nenhuma mensagem relevante para enviar.")
        return

    mensagem_final = "Segue as LH´s com mais tempo de Pátio:\n\n" + "\n\n".join(mensagem)
    enviar_webhook(mensagem_final)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ Erro Crítico Main: {e}")
