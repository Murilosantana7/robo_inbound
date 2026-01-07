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

    # --- TÉCNICA DO SCRIPT ANTIGO + PROTEÇÃO ---
    # 1. Pega os cabeçalhos brutos
    headers_originais = [str(h).strip() for h in valores[0]]
    
    # 2. Cria cabeçalhos únicos (Status, Status_1, Status_2...)
    # Isso simula uma planilha "limpa" para o Pandas não travar
    headers_unicos = []
    seen = {}
    for h in headers_originais:
        if h in seen:
            seen[h] += 1
            headers_unicos.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            headers_unicos.append(h)
            
    # 3. Cria o DataFrame usando esses cabeçalhos seguros
    df = pd.DataFrame(valores[1:], columns=headers_unicos)
    print("ℹ️ Planilha carregada e colunas duplicadas tratadas.")

    # --- MAPEAMENTO SEGURO (Baseado no seu script antigo) ---
    try:
        # Pega os nomes exatos que ficaram nas posições chaves
        # O script antigo confiava na posição, vamos fazer igual
        nome_col_eta = headers_unicos[1]    # Coluna B (índice 1)
        nome_col_pacotes = headers_unicos[5] # Coluna F (índice 5)
        
        # Renomeia para um padrão interno nosso para facilitar
        df.rename(columns={
            nome_col_eta: 'ETA Planejado',
            nome_col_pacotes: 'Pacotes'
        }, inplace=True)

        # Procura coluna Origem (AC - índice 28) se existir
        if len(headers_unicos) > 28:
            nome_col_origem = headers_unicos[28]
            df.rename(columns={nome_col_origem: 'Origem'}, inplace=True)
    except IndexError:
        print("❌ A planilha mudou de tamanho e não tem as colunas esperadas.")
        return

    # Limpeza de Strings nas colunas que vamos usar
    cols_para_limpar = ['LH Trip Nnumber', 'Satus 2.0', 'Doca', 'Turno 2']
    for col in cols_para_limpar:
        # Procura se a coluna existe (ou se virou Satus 2.0_1, etc)
        col_existente = None
        if col in df.columns:
            col_existente = col
        else:
            # Tenta achar a variação renomeada
            for c in df.columns:
                if c.startswith(col):
                    col_existente = c
                    break
        
        if col_existente:
            # Normaliza o nome para o script usar sempre o nome padrão
            if col_existente != col:
                df.rename(columns={col_existente: col}, inplace=True)
            df[col] = df[col].astype(str).str.strip()

    # --- DATAS (USANDO NÚMERO DA COLUNA COMO VOCÊ PEDIU) ---
    print("ℹ️ Processando datas de Chegada (Colunas D e G)...")
    
    # Acessa diretamente pelo número da coluna (iloc), ignorando nomes repetidos
    # Coluna D = Índice 3 | Coluna G = Índice 6
    col_d = pd.to_datetime(df.iloc[:, 3], dayfirst=True, errors='coerce')
    col_g = pd.to_datetime(df.iloc[:, 6], dayfirst=True, errors='coerce')
    
    df['Chegada LT'] = col_d.combine_first(col_g)
    
    # Outras datas
    if 'Add to Queue Time' in df.columns:
        df['Add to Queue Time'] = pd.to_datetime(df['Add to Queue Time'], dayfirst=True, errors='coerce')
    df['ETA Planejado'] = pd.to_datetime(df['ETA Planejado'], dayfirst=True, errors='coerce')
    df['Pacotes'] = pd.to_numeric(df['Pacotes'], errors='coerce').fillna(0).astype(int)

    # --- LÓGICA DE FILTROS ---
    if 'Satus 2.0' in df.columns:
        df['Satus 2.0'] = df['Satus 2.0'].replace({'Pendente Recepção': 'pendente recepção', 'Pendente De Chegada': 'pendente de chegada'})
        # Aqui era onde o erro acontecia. Agora com colunas únicas, não acontece mais.
        df = df[~df['Satus 2.0'].fillna('').str.lower().str.contains('finalizado')]

    agora_utc = datetime.utcnow().replace(second=0, microsecond=0)
    inicio_dia, fim_dia = periodo_dia_customizado(agora_utc)
    
    em_doca, em_fila, pendentes_por_turno = [], [], {}
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        # Usa .get para segurança caso alguma coluna tenha sumido
        trip = row.get('LH Trip Nnumber', '???')
        status = str(row.get('Satus 2.0', '')).strip().lower()
        origem = row.get('Origem', '--')
        if pd.isna(origem) or str(origem).strip() == '': origem = '--'
        
        # Logica Pendentes
        eta = row.get('ETA Planejado')
        if status in pendentes_status and pd.notna(eta) and inicio_dia <= eta <= fim_dia:
            t = row.get('Turno 2', 'Indef')
            if t not in pendentes_por_turno: pendentes_por_turno[t] = {'lts': 0, 'pacotes': 0}
            pendentes_por_turno[t]['lts'] += 1
            pendentes_por_turno[t]['pacotes'] += row.get('Pacotes', 0)

        # Logica Doca/Fila
        entrada = row.get('Add to Queue Time')
        eta_str = eta.strftime('%d/%m %H:%M') if pd.notna(eta) else '--/-- --:--'
        
        chegada_val = row.get('Chegada LT')
        chegada_str = chegada_val.strftime('%d/%m %H:%M') if pd.notna(chegada_val) else '--/-- --:--'
        
        doca_val = row.get('Doca', '--')
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
