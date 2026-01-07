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

# --- FUNÇÃO DE ESPERA ("O PORTÃO") ---
def aguardar_horario_correto():
    """
    Verifica se é hora cheia (XX:00) ou meia hora (XX:30).
    Se não for, aguarda até o próximo intervalo.
    """
    print(f"Iniciando verificação de horário às {datetime.utcnow().strftime('%H:%M:%S')} (Fuso UTC do Servidor)")
    
    while True:
        agora_utc = datetime.utcnow()
        minutos_atuais = agora_utc.minute
        
        # Verifica se é hora cheia (00) ou meia hora (30)
        if minutos_atuais == 0 or minutos_atuais == 30:
            print(f"✅ 'Portão' aberto: {agora_utc.strftime('%H:%M:%S')} UTC")
            print("Iniciando coleta de dados...")
            break 
        else:
            if minutos_atuais < 30:
                minutos_faltando = 30 - minutos_atuais
                proximo_horario_str = f"{agora_utc.hour:02d}:30"
            else:
                minutos_faltando = 60 - minutos_atuais
                proxima_hora = (agora_utc.hour + 1) % 24
                proximo_horario_str = f"{proxima_hora:02d}:00"
            
            segundos_para_o_proximo_check = 30 - (agora_utc.second % 30)
            print(f"⏳ Horário atual: {agora_utc.strftime('%H:%M:%S')} UTC")
            print(f"   Aguardando o 'portão' abrir às {proximo_horario_str} (faltam ~{minutos_faltando} min)")
            
            time.sleep(segundos_para_o_proximo_check)

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
    
    # --- MUDANÇA AQUI: LIMITE ALTERADO PARA 4000 ---
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
        origem = str(row.get(COL_ORIGEM, '--')).strip()
        if not origem: origem = "--"
        
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
            
            # --- TABELA RESTAURADA ---
            linha_tabela = f"{trip:^13} | {doca_limpa:^4} | {eta_str:^11} | {chegada_str:^11} | {tempo_fmt:^6} | {origem}"
            
            if 'fila' in status:
                em_fila.append((minutos, linha_tabela))
            elif status == 'em doca':
                em_doca.append((minutos, linha_tabela))

    # --- ORDENAÇÃO ---
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)

    mensagem = []
    
    # Cabeçalho
    header_tabela = f"{'LT':^13} | {'Doca':^4} | {'ETA':^11} | {'Chegada':^11} | {'Tempo':^6} | Origem"

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
    print("📤 Enviando mensagem...")
    enviar_webhook(msg_final)

if __name__ == '__main__':
    aguardar_horario_correto()
    
    try:
        main()
    except Exception as e:
        print(f"❌ Erro Fatal: {e}")
        try:
            enviar_webhook(f"Erro Crítico Script: {e}")
        except:
            pass
