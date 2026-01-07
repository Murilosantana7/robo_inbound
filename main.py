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

def turno_atual():
    agora = datetime.utcnow().time()
    if agora >= dt_time(6, 0) and agora < dt_time(14, 0): return "T1"
    elif agora >= dt_time(14, 0) and agora < dt_time(22, 0): return "T2"
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
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

# --- Função Principal ---
def main():
    print(f"🔄 Script 'main' iniciado.")
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

    # --- TRATAMENTO DE CABEÇALHOS DUPLICADOS ---
    # Antes de criar o DataFrame, vamos garantir que os nomes sejam únicos
    headers = [h.strip() for h in valores[0]]
    seen = {}
    unique_headers = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            unique_headers.append(f"{h}_{seen[h]}") # Ex: Status_1
        else:
            seen[h] = 0
            unique_headers.append(h)
    
    # Cria o DataFrame com os cabeçalhos únicos
    df = pd.DataFrame(valores[1:], columns=unique_headers)
    print("ℹ️ Cabeçalhos processados e duplicatas renomeadas.")

    # Mapeamento seguro (usando os nomes únicos ou originais)
    # Aqui, a lógica de 'só usar o número' é aplicada acessando o que está na posição correta
    try:
        # Recupera os nomes exatos que ficaram nas posições esperadas
        # Coluna B (Indice 1)
        name_eta = unique_headers[1]
        # Coluna F (Indice 5)
        name_pacotes = unique_headers[5]
        # Coluna AC (Indice 28) - Verificando se existe
        name_origem = unique_headers[28] if len(unique_headers) > 28 else None
    except IndexError:
        print("❌ Erro: Planilha com menos colunas do que o esperado.")
        return

    # Renomeia para padronizar o uso no script
    rename_map = {
        name_eta: 'ETA Planejado',
        name_pacotes: 'Pacotes'
    }
    if name_origem:
        rename_map[name_origem] = 'Origem'
        
    df.rename(columns=rename_map, inplace=True)
    
    # Tratamento de Strings
    # Verifica se as colunas existem antes de tentar converter
    for col in ['LH Trip Nnumber', 'Satus 2.0', 'Doca', 'Turno 2']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        else:
            # Tenta achar variação com sufixo (ex: Satus 2.0_1)
            found = False
            for c in df.columns:
                if c.startswith(col):
                    df.rename(columns={c: col}, inplace=True)
                    df[col] = df[col].astype(str).str.strip()
                    found = True
                    break
            if not found:
                print(f"⚠️ Aviso: Coluna '{col}' não encontrada (nem duplicada).")

    # --- LÓGICA DE DATAS DE CHEGADA (PELO NÚMERO DA COLUNA) ---
    print("ℹ️ Processando datas de Chegada (Colunas D e G)...")
    # Coluna D = Índice 3 | Coluna G = Índice 6
    # Acessa diretamente pelo número (iloc), ignorando totalmente o nome
    col_d_convertida = pd.to_datetime(df.iloc[:, 3], dayfirst=True, errors='coerce')
    col_g_convertida = pd.to_datetime(df.iloc[:, 6], dayfirst=True, errors='coerce')
    
    df['Chegada LT'] = col_d_convertida.combine_first(col_g_convertida)
    # -----------------------------------------------------------

    # Outras conversões de Data
    if 'Add to Queue Time' in df.columns:
        df['Add to Queue Time'] = pd.to_datetime(df['Add to Queue Time'], dayfirst=True, errors='coerce')
    
    df['ETA Planejado'] = pd.to_datetime(df['ETA Planejado'], dayfirst=True, errors='coerce')
    df['Pacotes'] = pd.to_numeric(df['Pacotes'], errors='coerce').fillna(0).astype(int)

    # Filtros
    if 'Satus 2.0' in df.columns:
        df['Satus 2.0'] = df['Satus 2.0'].replace({'Pendente Recepção': 'pendente recepção', 'Pendente De Chegada': 'pendente de chegada'})
        # Filtro finalizado
        df = df[~df['Satus 2.0'].fillna('').str.lower().str.contains('finalizado')]

    agora_utc = datetime.utcnow().replace(second=0, microsecond=0)
    inicio_dia, fim_dia = periodo_dia_customizado(agora_utc)
    
    em_doca, em_fila, pendentes_por_turno = [], [], {}
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        # Verifica existência das colunas antes de ler
        trip = row['LH Trip Nnumber'] if 'LH Trip Nnumber' in df.columns else '???'
        status = str(row['Satus 2.0']).strip().lower() if 'Satus 2.0' in df.columns else ''
        origem = row['Origem'] if 'Origem' in df.columns and pd.notna(row['Origem']) and str(row['Origem']).strip() != '' else '--'
        
        # Logica Pendentes
        if status in pendentes_status and pd.notna(row['ETA Planejado']) and inicio_dia <= row['ETA Planejado'] <= fim_dia:
            t = row['Turno 2'] if 'Turno 2' in df.columns else 'Indef'
            if t not in pendentes_por_turno: pendentes_por_turno[t] = {'lts': 0, 'pacotes': 0}
            pendentes_por_turno[t]['lts'] += 1
            pendentes_por_turno[t]['pacotes'] += row['Pacotes']

        # Logica Doca/Fila
        entrada = row['Add to Queue Time'] if 'Add to Queue Time' in df.columns else pd.NaT
        eta_str = row['ETA Planejado'].strftime('%d/%m %H:%M') if pd.notna(row['ETA Planejado']) else '--/-- --:--'
        
        chegada_val = row['Chegada LT']
        chegada_str = chegada_val.strftime('%d/%m %H:%M') if pd.notna(chegada_val) else '--/-- --:--'
        
        doca_val = row['Doca'] if 'Doca' in df.columns else '--'
        doca_limpa = padronizar_doca(str(doca_val))

        minutos = None
        if pd.notna(entrada):
            minutos = int((agora_utc - entrada).total_seconds() / 60)

        if minutos is not None:
            tempo_fmt = minutos_para_hhmm(minutos)
            linha_msg = f"- {trip} | Doca: {doca_limpa} | ETA: {eta_str} | Chegada: {chegada_str} | Tempo: {tempo_fmt} | {origem}"
            
            if 'fila' in status:
                linha_msg = f"- {trip} | ETA: {eta_str} | Chegada: {chegada_str} | Tempo: {tempo_fmt} | {origem}"
                em_fila.append((minutos, linha_msg))
            elif status == 'em doca':
                em_doca.append((minutos, linha_msg))

    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)

    mensagem = []

    if em_doca:
        qtd = len(em_doca)
        texto = "\n".join([x[1] for x in em_doca])
        mensagem.append(f"🚛 Em Doca: {qtd} LT(s)\n{texto}")

    if em_fila:
        qtd = len(em_fila)
        texto = "\n".join([x[1] for x in em_fila])
        mensagem.append(f"🔴 Em Fila: {qtd} LT(s)\n{texto}")

    total_pend = sum(d['lts'] for d in pendentes_por_turno.values())
    if total_pend > 0:
        pcts = sum(d['pacotes'] for d in pendentes_por_turno.values())
        mensagem.append(f"⏳ Pendentes: {total_pend} LTs ({pcts} pct)")
        for t, d in ordenar_turnos(pendentes_por_turno):
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
