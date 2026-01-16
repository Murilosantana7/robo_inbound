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
        # Tenta decodificar se estiver em Base64
        decoded_bytes = base64.b64decode(creds_raw, validate=True)
        creds_json_str = decoded_bytes.decode('utf-8')
        print("ℹ️ Credencial detectada como Base64 e decodificada.")
    except (binascii.Error, ValueError):
        # Se falhar, assume que já é o JSON puro
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
    
    LIMITE_SEGURO = 4000 
    partes = []
    
    # 1. Só divide se for estritamente necessário
    if len(mensagem_txt) > LIMITE_SEGURO:
        print(f"⚠️ Mensagem excedeu {LIMITE_SEGURO} chars. Dividindo em partes...")
        while len(mensagem_txt) > 0:
            if len(mensagem_txt) > LIMITE_SEGURO:
                # Procura a última quebra de linha antes do limite
                corte = mensagem_txt.rfind('\n', 0, LIMITE_SEGURO)
                if corte == -1: corte = LIMITE_SEGURO 
                
                partes.append(mensagem_txt[:corte])
                mensagem_txt = mensagem_txt[corte:] 
            else:
                partes.append(mensagem_txt)
                break
    else:
        # Manda inteiro
        partes.append(mensagem_txt)

    # 2. Envio
    for i, parte in enumerate(partes):
        print(f"📤 Enviando parte {i+1}/{len(partes)}...")
        try:
            texto_final = parte
            if len(partes) > 1:
                texto_final = f"({i+1}/{len(partes)})\n{parte}"

            payload = {
                "tag": "text",
                "text": { "format": 1, "content": f"```\n{texto_final}\n```" }
            }
            response = requests.post(webhook_url, json=payload)
            response.raise_for_status()
            time.sleep(1) 
        except Exception as e:
            print(f"❌ Erro ao enviar parte {i+1}: {e}")

    print("✅ Processo de envio finalizado.")

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
    print(f"🔄 Script 'main' iniciado.")
    
    # Define Horário Brasil (UTC-3)
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
    
    # Atenção: Nome exato da coluna na dinâmica
    COL_PACOTES = 'SUM de total_orders' 
    
    COL_STATUS  = 'Status'
    COL_TURNO   = 'Turno'
    COL_DOCA    = 'Doca'
    COL_CUTOFF  = 'Cutoff'

    # Tratamento de headers duplicados
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
    # Conversões e limpezas
    if COL_CHECKIN in df.columns:
        df[COL_CHECKIN] = pd.to_datetime(df[COL_CHECKIN], dayfirst=True, errors='coerce')
    else:
        # Fallback por índice (Cuidado se a coluna mudar de lugar)
        if len(df.columns) > 3: df[COL_CHECKIN] = pd.to_datetime(df.iloc[:, 3], dayfirst=True, errors='coerce')

    if COL_ENTRADA in df.columns:
        df[COL_ENTRADA] = pd.to_datetime(df[COL_ENTRADA], dayfirst=True, errors='coerce')
    else:
         if len(df.columns) > 6: df[COL_ENTRADA] = pd.to_datetime(df.iloc[:, 6], dayfirst=True, errors='coerce')

    if COL_ETA in df.columns:
        df[COL_ETA] = pd.to_datetime(df[COL_ETA], dayfirst=True, errors='coerce')

    if COL_CUTOFF in df.columns:
        df[COL_CUTOFF] = pd.to_datetime(df[COL_CUTOFF], dayfirst=True, errors='coerce')
    
    if COL_PACOTES in df.columns:
        df[COL_PACOTES] = pd.to_numeric(df[COL_PACOTES], errors='coerce').fillna(0).astype(int)

    if COL_STATUS in df.columns:
        df[COL_STATUS] = df[COL_STATUS].astype(str).str.strip()
        df[COL_STATUS] = df[COL_STATUS].replace({'Pendente Recepção': 'pendente recepção', 'Pendente De Chegada': 'pendente de chegada'})
        df = df[~df[COL_STATUS].fillna('').str.lower().str.contains('finalizado')]

    # --- LÓGICA DE DATAS E TURNOS ---
    # Define data operacional (se antes das 06:00, conta como dia anterior)
    if agora_br.time() < dt_time(6, 0):
        op_date_hoje = agora_br.date() - timedelta(days=1)
    else:
        op_date_hoje = agora_br.date()

    op_date_amanha = op_date_hoje + timedelta(days=1)
    
    hora_atual = agora_br.time()
    turno_atual_str = "T3" # Default
    if dt_time(6, 0) <= hora_atual < dt_time(14, 0):
        turno_atual_str = "T1"
    elif dt_time(14, 0) <= hora_atual < dt_time(22, 0):
        turno_atual_str = "T2"
        
    mapa_turnos = {'T1': 1, 'T2': 2, 'T3': 3}
    peso_turno_atual = mapa_turnos.get(turno_atual_str, 0)
    
    em_doca, em_fila = [], []
    resumo = {
        'atrasado': {},
        'hoje': {},
        'amanha': {}
    }
    
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        trip = str(row.get(COL_TRIP, '???')).strip()
        status = str(row.get(COL_STATUS, '')).strip().lower()
        origem = str(row.get(COL_ORIGEM, '--')).strip()
        if not origem: origem = "--"
        
        eta = row.get(COL_ETA)
        cutoff = row.get(COL_CUTOFF)
        val_checkin = row.get(COL_CHECKIN)
        qtd_pacotes = row.get(COL_PACOTES, 0)

        # --- FILTRO 1: REMOVE VAZIOS (NOVA REGRA) ---
        # Se a quantidade de pacotes for 0 ou negativa, ignora totalmente
        if qtd_pacotes <= 0:
            continue

        # --- FILTRO 2: LIMPEZA DE ANTIGOS SEM CHECKIN ---
        if pd.notna(cutoff):
            d_cutoff = cutoff.date()
            if d_cutoff < op_date_hoje and pd.isna(val_checkin):
                continue 
        
        # --- LÓGICA DE CLASSIFICAÇÃO (Atrasado/Hoje/Amanhã) ---
        if status in pendentes_status:
            t = str(row.get(COL_TURNO, 'Indef')).strip()
            
            categoria = None
            
            if pd.notna(cutoff):
                d_cutoff = cutoff.date()
                
                if d_cutoff < op_date_hoje:
                    categoria = 'atrasado'
                    
                elif d_cutoff == op_date_hoje:
                    peso_turno_row = mapa_turnos.get(t, 99) 
                    
                    if peso_turno_row < peso_turno_atual:
                        categoria = 'atrasado'
                    else:
                        categoria = 'hoje'      
                        
                elif d_cutoff == op_date_amanha:
                    categoria = 'amanha'
            else:
                categoria = 'hoje' 
            
            if categoria:
                if t not in resumo[categoria]: 
                    resumo[categoria][t] = {'lts': 0, 'pacotes': 0}
                resumo[categoria][t]['lts'] += 1
                resumo[categoria][t]['pacotes'] += qtd_pacotes

        # --- LÓGICA EM DOCA / EM FILA ---
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
            linha_tabela = f"{trip:^13} | {doca_limpa:^4} | {eta_str:^11} | {chegada_str:^11} | {tempo_fmt:^6} | {origem}"
            
            if 'fila' in status:
                em_fila.append((minutos, linha_tabela))
            elif status == 'em doca':
                em_doca.append((minutos, linha_tabela))

    # --- MONTAGEM DA MENSAGEM ---
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)

    mensagem = []
    header_tabela = f"{'LT':^13} | {'Doca':^4} | {'ETA':^11} | {'Chegada':^11} | {'Tempo':^6} | Origem"

    if em_doca:
        qtd = len(em_doca)
        texto = "\n".join([x[1] for x in em_doca])
        mensagem.append(f"🚛 Em Doca: {qtd} LT(s)\n{header_tabela}\n{texto}")

    if em_fila:
        qtd = len(em_fila)
        texto = "\n".join([x[1] for x in em_fila])
        mensagem.append(f"🔴 Em Fila: {qtd} LT(s)\n{header_tabela}\n{texto}")

    str_amanha = op_date_amanha.strftime('%d/%m/%Y')
    
    titulos = {
        'atrasado': '⚠️ Atrasados',
        'hoje': '📅 Hoje',
        'amanha': f'🌅 Amanhã {str_amanha}'
    }
    
    ordem_turnos = ['T1', 'T2', 'T3']
    ordem_exibicao = ['atrasado', 'hoje', 'amanha']

    for cat in ordem_exibicao:
        dados_cat = resumo[cat]
        if dados_cat:
            total_cat = sum(d['lts'] for d in dados_cat.values())
            pcts_cat = sum(d['pacotes'] for d in dados_cat.values())
            
            bloco = [f"{titulos[cat]}: {total_cat} LTs ({pcts_cat} pct)"]
            
            turnos_ordenados = sorted(dados_cat.items(), key=lambda x: ordem_turnos.index(x[0]) if x[0] in ordem_turnos else 99)
            
            for t, d in turnos_ordenados:
                bloco.append(f"   - {t}: {d['lts']} LTs ({d['pacotes']} pct)")
            
            mensagem.append("\n".join(bloco))

    if not mensagem:
        print("ℹ️ Nada a enviar.")
        return

    msg_final = "Segue as LH´s com mais tempo de Pátio:\n\n" + "\n\n".join(mensagem)
    print("📤 Enviando mensagem...")
    enviar_webhook(msg_final)

if __name__ == '__main__':
    # A função 'aguardar_horario_correto' foi removida para otimização
    
    try:
        main()
    except Exception as e:
        print(f"❌ Erro Fatal: {e}")
        try:
            enviar_webhook(f"Erro Crítico Script: {e}")
        except:
            pass
