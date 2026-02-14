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
        payload = {
            "tag": "text",
            "text": { "format": 1, "content": f"```\n{mensagem_txt}\n```" }
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
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

def ler_aba_com_retry(planilha, nome_aba, range_celulas):
    """Função auxiliar para ler abas com segurança"""
    for tentativa in range(3):
        try:
            dados = planilha.worksheet(nome_aba).get(range_celulas)
            if len(dados) > 1: return dados
            else:
                print(f"⚠️ Aba '{nome_aba}' parece vazia ou atualizando. (Tentativa {tentativa+1}/3)")
                time.sleep(3)
                if tentativa == 2: return dados
        except Exception as e:
            print(f"❌ Erro ao ler '{nome_aba}': {e}")
            time.sleep(3)
    return []

# --- Lógica Principal ---
def main():
    print(f"🔄 Iniciando processamento...")
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

    # =========================================================================
    # PARTE 1: Processar o PÁTIO (Aba 'Report')
    # =========================================================================
    raw_report = ler_aba_com_retry(planilha, 'Report', 'A1:K8000')
    
    em_descarregando, em_doca, em_fila = [], [], []
    
    if raw_report:
        df_rep = pd.DataFrame(raw_report[1:], columns=[str(h).strip() for h in raw_report[0]])
        
        # Mapeamento Report
        C_TRIP    = 'LH Trip Nnumber' 
        C_ETA     = 'ETA Planejado'
        C_ORIGEM  = 'station_code'
        C_CHECKIN = 'Checkin'
        C_ENTRADA = 'Add to Queue Time'
        C_STATUS  = 'Status'
        C_DOCA    = 'Doca'

        # Converter datas
        for col in [C_CHECKIN, C_ENTRADA, C_ETA]:
            if col in df_rep.columns:
                df_rep[col] = pd.to_datetime(df_rep[col], dayfirst=True, errors='coerce')

        for _, row in df_rep.iterrows():
            status = str(row.get(C_STATUS, '')).strip().lower()
            
            # Filtra apenas o que interessa para o pátio
            if any(s in status for s in ['descarregando', 'doca', 'fila']) and 'finalizado' not in status:
                
                data_ref = row[C_CHECKIN] if pd.notna(row[C_CHECKIN]) else row[C_ENTRADA]
                
                trip = str(row.get(C_TRIP, '???')).strip()
                doca = padronizar_doca(str(row.get(C_DOCA, '--')))
                eta_s = row[C_ETA].strftime('%d/%m %H:%M') if pd.notna(row[C_ETA]) else '--/-- --:--'
                che_s = data_ref.strftime('%d/%m %H:%M') if pd.notna(data_ref) else '--/-- --:--'
                
                minutos = int((agora_br - data_ref).total_seconds() / 60) if pd.notna(data_ref) else -999999
                tempo = minutos_para_hhmm(minutos)
                
                linha = f"{trip:^13} | {doca:^4} | {eta_s:^11} | {che_s:^11} | {tempo:^6} | {str(row.get(C_ORIGEM, '--'))}"
                
                if 'descarregando' in status: em_descarregando.append((minutos, linha))
                elif 'doca' in status: em_doca.append((minutos, linha))
                elif 'fila' in status: em_fila.append((minutos, linha))

    # =========================================================================
    # PARTE 2: Processar o RESUMO (Aba 'Pendente')
    # =========================================================================
    raw_pendente = ler_aba_com_retry(planilha, 'Pendente', 'A1:D8000') # Colunas A, B, C, D
    
    resumo = {'atrasado': {}, 'hoje': {}, 'amanha': {}}
    
    # Definição de datas operacionais
    if agora_br.time() < dt_time(6, 0): op_date_hoje = agora_br.date() - timedelta(days=1)
    else: op_date_hoje = agora_br.date()
    op_date_amanha = op_date_hoje + timedelta(days=1)
    
    hora_atual = agora_br.time()
    turno_atual_str = "T3"
    if dt_time(6, 0) <= hora_atual < dt_time(14, 0): turno_atual_str = "T1"
    elif dt_time(14, 0) <= hora_atual < dt_time(22, 0): turno_atual_str = "T2"
    mapa_turnos = {'T1': 1, 'T2': 2, 'T3': 3}

    if raw_pendente:
        # Colunas esperadas: LT (A), Data (B), Turno (C), Pacotes (D)
        df_pen = pd.DataFrame(raw_pendente[1:], columns=[str(h).strip() for h in raw_pendente[0]])
        
        # Ajuste nomes se necessário (case insensitive)
        df_pen.columns = [c.capitalize() for c in df_pen.columns] # Ex: 'pacotes' vira 'Pacotes'
        
        # Garante nomes corretos baseados na sua imagem
        # Se na planilha for "Data", o script lê "Data".
        
        df_pen['Pacotes'] = pd.to_numeric(df_pen['Pacotes'], errors='coerce').fillna(0).astype(int)
        df_pen['Data'] = pd.to_datetime(df_pen['Data'], dayfirst=True, errors='coerce')
        
        for _, row in df_pen.iterrows():
            if pd.isna(row['Data']): continue # Pula se não tiver data
            
            t = str(row.get('Turno', 'Indef')).strip().upper()
            pct = row['Pacotes']
            d_alvo = row['Data'].date()
            
            categoria = None
            if d_alvo < op_date_hoje: 
                categoria = 'atrasado'
            elif d_alvo == op_date_hoje:
                # Se for hoje, mas o turno já passou, é atrasado
                eh_turno_passado = mapa_turnos.get(t, 99) < mapa_turnos.get(turno_atual_str, 0)
                categoria = 'atrasado' if eh_turno_passado else 'hoje'
            elif d_alvo == op_date_amanha: 
                categoria = 'amanha'
            
            if categoria:
                if t not in resumo[categoria]: resumo[categoria][t] = {'lts': 0, 'pacotes': 0}
                resumo[categoria][t]['lts'] += 1
                resumo[categoria][t]['pacotes'] += pct

    # =========================================================================
    # MONTAGEM DA MENSAGEM
    # =========================================================================
    em_descarregando.sort(key=lambda x: x[0], reverse=True)
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)
    
    header = f"{'LT':^13} | {'Doca':^4} | {'ETA':^11} | {'Chegada':^11} | {'Tempo':^6} | Origem"
    bloco_patio = ["Segue as LH´s com mais tempo de Pátio:\n"]
    
    if em_descarregando:
        bloco_patio.append(f"📦 Descarregando: {len(em_descarregando)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_descarregando])
        
    if em_doca:
        prefixo = "\n" if em_descarregando else ""
        bloco_patio.append(f"{prefixo}🚛 Em Doca: {len(em_doca)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_doca])
        
    if em_fila:
        prefixo = "\n" if (em_descarregando or em_doca) else ""
        bloco_patio.append(f"{prefixo}🔴 Em Fila: {len(em_fila)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_fila])

    # Monta Resumo
    bloco_resumo = []
    str_amanha = op_date_amanha.strftime('%d/%m/%Y')
    titulos = {'atrasado': '⚠️ Atrasados', 'hoje': '📅 Hoje', 'amanha': f'🌅 Amanhã {str_amanha}'}
    
    for cat in ['atrasado', 'hoje', 'amanha']:
        # Verifica se tem dados nessa categoria
        tem_dados = len(resumo[cat]) > 0
        
        # Calcula totais
        total_lts = sum(d['lts'] for d in resumo[cat].values())
        total_pct = sum(d['pacotes'] for d in resumo[cat].values())
        
        # Adiciona título (exibe mesmo que esteja zerado, ou você pode colocar 'if tem_dados:')
        bloco_resumo.append(f"{titulos[cat]}: {total_lts} LTs ({total_pct} pcts)")
        
        # Ordena turnos T1, T2, T3
        for t in sorted(resumo[cat].keys()):
            r = resumo[cat][t]
            bloco_resumo.append(f"   - {t}: {r['lts']} LTs ({r['pacotes']} pcts)")
        
        bloco_resumo.append("") # Linha em branco entre blocos

    # Envio
    txt_patio = "\n".join(bloco_patio)
    txt_resumo = "\n".join(bloco_resumo)
    linha_divisoria = "\n" + ("-" * 72) + "\n\n"
    
    txt_completo = txt_patio + linha_divisoria + txt_resumo

    print("📤 Enviando...")
    if not enviar_webhook(txt_completo):
        print("✂️ Dividindo mensagem...")
        enviar_webhook(txt_patio)
        time.sleep(1)
        if txt_resumo: enviar_webhook(txt_resumo)
    else:
        print("✅ Sucesso!")

if __name__ == '__main__':
    main()
