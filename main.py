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
    for tentativa in range(3):
        try:
            dados = planilha.worksheet(nome_aba).get(range_celulas)
            if len(dados) > 1:
                return dados
            else:
                print(f"⚠️ Aba '{nome_aba}' parece vazia ou atualizando. (Tentativa {tentativa+1}/3)")
                time.sleep(3)
                if tentativa == 2:
                    return dados
        except Exception as e:
            print(f"❌ Erro ao ler '{nome_aba}': {e}")
            time.sleep(3)
    return []

# --- Lógica Principal ---
def main():
    # Modo Teste: Roda imediatamente sem esperar horário
    print(f"🔄 Iniciando processamento de dados (Ajuste Origem Col F + Filtro Atrasados)...")
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

    # Listas finais
    em_descarregando, em_doca, em_fila, em_chegada = [], [], [], []
    
    # Set para guardar LTs que JÁ apareceram no Report (para não duplicar no Deu Chegada)
    lts_processados_no_report = set()

    # =========================================================================
    # PARTE 1: Processar o PÁTIO (Aba 'Report') - PRIORIDADE TOTAL
    # =========================================================================
    raw_report = ler_aba_com_retry(planilha, 'Report', 'A1:L8000')
    
    if raw_report:
        colunas = [str(h).strip() for h in raw_report[0]]
        df_rep = pd.DataFrame(raw_report[1:], columns=colunas)
        
        C_TRIP    = 'LH Trip Nnumber' 
        C_ETA     = 'ETA Planejado'
        C_ORIGEM  = 'station_code'
        C_CHECKIN = 'Checkin'
        C_ENTRADA = 'Add to Queue Time'
        C_STATUS  = 'Status'
        C_DOCA    = 'Doca'
        C_TO      = 'TO'

        # Converter datas
        for col in [C_CHECKIN, C_ENTRADA, C_ETA]:
            if col in df_rep.columns:
                df_rep[col] = pd.to_datetime(df_rep[col], dayfirst=True, errors='coerce')

        for _, row in df_rep.iterrows():
            status = str(row.get(C_STATUS, '')).strip().lower()
            
            # Filtro normal do Report
            termos_interesse = ['descarregando', 'doca', 'fila']
            
            if any(s in status for s in termos_interesse) and 'finalizado' not in status:
                
                lt_atual = str(row.get(C_TRIP, '???')).strip()
                
                # ADICIONA AO SET DE LTs PROCESSADOS para exclusão posterior
                if lt_atual and lt_atual != '???':
                    lts_processados_no_report.add(lt_atual)

                data_ref = row[C_CHECKIN] if pd.notna(row.get(C_CHECKIN)) else row.get(C_ENTRADA)
                
                doca = padronizar_doca(str(row.get(C_DOCA, '--')))
                val_to = str(row.get(C_TO, '--')).strip()
                origem = str(row.get(C_ORIGEM, '--')).strip() 
                
                eta_val = row.get(C_ETA)
                eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'
                
                if pd.notna(data_ref):
                    minutos = int((agora_br - data_ref).total_seconds() / 60)
                else:
                    minutos = -999999
                    
                tempo = minutos_para_hhmm(minutos)
                
                linha = f"{lt_atual:^13} | {doca:^4} | {val_to:^7} | {eta_s:^11} | {tempo:^6} | {origem:^10}"
                
                if 'descarregando' in status:
                    em_descarregando.append((minutos, linha))
                elif 'doca' in status:
                    em_doca.append((minutos, linha))
                elif 'fila' in status:
                    em_fila.append((minutos, linha))

    # =========================================================================
    # PARTE 2: Processar 'Deu chegada' (Apenas o que NÃO estava no Report)
    # =========================================================================
    # Lendo colunas A até F para pegar LT, TO, ETA, Chegada e CODE (F)
    raw_chegada_manual = ler_aba_com_retry(planilha, 'Deu chegada', 'A1:F1000')

    if raw_chegada_manual:
        cols_manual = [str(h).strip() for h in raw_chegada_manual[0]]
        df_manual = pd.DataFrame(raw_chegada_manual[1:], columns=cols_manual)
        
        # Mapeamento de colunas da aba manual
        col_lt_m = next((c for c in df_manual.columns if c.upper() == 'LT'), 'LT')
        
        # ALTERAÇÃO 1: Mapear 'Origem' para a coluna 'code' (que é a F no print)
        col_origem_m = next((c for c in df_manual.columns if 'code' in c.lower()), 'code')
        
        col_tos_m = next((c for c in df_manual.columns if 'TOs' in c), 'TOs')
        col_eta_m = next((c for c in df_manual.columns if 'ETA' in c), 'ETA Planejado')
        col_chegada_m = next((c for c in df_manual.columns if 'Chegada' in c), 'Chegada') # Coluna E

        # Converte coluna de tempo
        if col_chegada_m in df_manual.columns:
            df_manual[col_chegada_m] = pd.to_datetime(df_manual[col_chegada_m], dayfirst=True, errors='coerce')
        if col_eta_m in df_manual.columns:
            df_manual[col_eta_m] = pd.to_datetime(df_manual[col_eta_m], dayfirst=True, errors='coerce')

        for _, row in df_manual.iterrows():
            lt_val = str(row.get(col_lt_m, '')).strip()
            time_val = row.get(col_chegada_m)
            
            # LÓGICA DE EXCLUSÃO:
            # Só processa se o LT existir E NÃO estiver na lista do Report (lts_processados_no_report)
            if lt_val and pd.notna(time_val) and (lt_val not in lts_processados_no_report):
                
                # Monta os dados com base na aba manual
                doca = "--" # Manual não tem doca
                val_to = str(row.get(col_tos_m, '--')).strip()
                origem = str(row.get(col_origem_m, '--')).strip() # Agora pega da coluna 'code'
                
                eta_val = row.get(col_eta_m)
                eta_s = eta_val.strftime('%d/%m %H:%M') if pd.notna(eta_val) else '--/-- --:--'
                
                # Calculo de tempo usando a coluna E (Chegada)
                minutos = int((agora_br - time_val).total_seconds() / 60)
                tempo = minutos_para_hhmm(minutos)
                
                linha = f"{lt_val:^13} | {doca:^4} | {val_to:^7} | {eta_s:^11} | {tempo:^6} | {origem:^10}"
                
                em_chegada.append((minutos, linha))

    # =========================================================================
    # PARTE 3: Processar o RESUMO (Aba 'Pendente')
    # =========================================================================
    raw_pendente = ler_aba_com_retry(planilha, 'Pendente', 'A1:F8000') 
    
    resumo = {'atrasado': {}, 'hoje': {}, 'amanha': {}}
    
    if agora_br.time() < dt_time(6, 0):
        op_date_hoje = agora_br.date() - timedelta(days=1)
    else:
        op_date_hoje = agora_br.date()
    op_date_amanha = op_date_hoje + timedelta(days=1)
    
    hora_atual = agora_br.time()
    turno_atual_str = "T3"
    if dt_time(6, 0) <= hora_atual < dt_time(14, 0): turno_atual_str = "T1"
    elif dt_time(14, 0) <= hora_atual < dt_time(22, 0): turno_atual_str = "T2"
    mapa_turnos = {'T1': 1, 'T2': 2, 'T3': 3}

    if raw_pendente:
        colunas_pen = [str(h).strip() for h in raw_pendente[0]]
        df_pen = pd.DataFrame(raw_pendente[1:], columns=colunas_pen)
        
        col_saida = next((c for c in df_pen.columns if 'descarregado' in c.lower()), None)
        col_pacotes = next((c for c in df_pen.columns if 'acote' in c.lower()), 'Pacotes')
        col_to = next((c for c in df_pen.columns if c.upper() == 'TO'), 'TO')
        col_data = next((c for c in df_pen.columns if 'ata' in c.lower()), 'Data')
        
        df_pen[col_pacotes] = pd.to_numeric(df_pen[col_pacotes], errors='coerce').fillna(0).astype(int)
        df_pen[col_to] = pd.to_numeric(df_pen[col_to], errors='coerce').fillna(0).astype(int)
        df_pen[col_data] = pd.to_datetime(df_pen[col_data], dayfirst=True, errors='coerce')
        
        for _, row in df_pen.iterrows():
            if pd.isna(row[col_data]): continue 
            
            if col_saida:
                val_saida = str(row.get(col_saida, '')).strip()
                if val_saida and val_saida.lower() != 'nan' and val_saida.lower() != 'none':
                    continue 

            t = str(row.get('Turno', 'Indef')).strip().upper()
            pct = row[col_pacotes]
            val_to_row = row[col_to]
            d_alvo = row[col_data].date()
            
            categoria = None
            if d_alvo < op_date_hoje: 
                categoria = 'atrasado'
            elif d_alvo == op_date_hoje:
                eh_turno_passado = mapa_turnos.get(t, 99) < mapa_turnos.get(turno_atual_str, 0)
                categoria = 'atrasado' if eh_turno_passado else 'hoje'
            elif d_alvo == op_date_amanha: 
                categoria = 'amanha'
            
            # ALTERAÇÃO 2: Se for atrasado, mas tiver 0 pacotes, ignora
            if categoria == 'atrasado' and pct == 0:
                categoria = None

            if categoria:
                if t not in resumo[categoria]: 
                    resumo[categoria][t] = {'lts': 0, 'pacotes': 0, 'tos': 0}
                resumo[categoria][t]['lts'] += 1
                resumo[categoria][t]['pacotes'] += pct
                resumo[categoria][t]['tos'] += val_to_row

    # =========================================================================
    # MONTAGEM DA MENSAGEM
    # =========================================================================
    em_descarregando.sort(key=lambda x: x[0], reverse=True)
    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)
    em_chegada.sort(key=lambda x: x[0], reverse=True)
    
    header = f"{'LT':^13} | {'Doca':^4} | {'TO':^7} | {'ETA':^11} | {'Tempo':^6} | {'Origem':^10}"
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

    # --- BLOCO DEU CHEGADA (ÚLTIMA PRIORIDADE E SÓ SE NÃO ESTIVER NO REPORT) ---
    if em_chegada:
        prefixo = "\n" if (em_descarregando or em_doca or em_fila) else ""
        bloco_patio.append(f"{prefixo}📢 Deu Chegada (Cobrar Monitoring): {len(em_chegada)} LT(s)\n{header}")
        bloco_patio.extend([x[1] for x in em_chegada])

    # Monta Resumo
    bloco_resumo = []
    str_amanha = op_date_amanha.strftime('%d/%m/%Y')
    titulos = {'atrasado': '⚠️ Atrasados', 'hoje': '📅 Hoje', 'amanha': f'🌅 Amanhã {str_amanha}'}
    
    for cat in ['atrasado', 'hoje', 'amanha']:
        if not resumo[cat]: continue

        total_lts = sum(d['lts'] for d in resumo[cat].values())
        total_pct = sum(d['pacotes'] for d in resumo[cat].values())
        total_tos = sum(d['tos'] for d in resumo[cat].values())
        
        bloco_resumo.append(f"{titulos[cat]}: {total_lts} LTs ({total_pct} pcts | {total_tos} TO)")
        
        for t in sorted(resumo[cat].keys()):
            r = resumo[cat][t]
            bloco_resumo.append(f"   - {t}: {r['lts']} LTs ({r['pacotes']} pcts | {r['tos']} TO)")
        
        bloco_resumo.append("") 

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
        if txt_resumo:
            enviar_webhook(txt_resumo)
    else:
        print("✅ Sucesso!")

if __name__ == '__main__':
    main()
