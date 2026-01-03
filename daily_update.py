import os
import requests
import urllib3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 1. 載入環境變數
load_dotenv()
DB_URI = os.getenv("DATABASE_URL")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ... (中間 get_db_connection, get_all_funds_from_db, get_last_date 都不用動) ...
# 為了版面整潔，請保留原本那三個函式，直接把下面這段 fetch_data_universal 和主程式換掉

def get_db_connection():
    if not DB_URI:
        print("❌ 錯誤：找不到 DATABASE_URL，請檢查 .env 檔案！")
        return None
    return psycopg2.connect(DB_URI)

def get_all_funds_from_db():
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM funds ORDER BY id;")
        funds = cursor.fetchall()
        return funds
    except Exception as e:
        print(f"❌ 讀取基金清單失敗: {e}")
        return []
    finally:
        conn.close()

def get_last_date(fund_id):
    conn = get_db_connection()
    if not conn: return None 
    try:
        cursor = conn.cursor()
        sql = "SELECT MAX(nav_date) FROM fund_navs WHERE fund_id = %s;"
        cursor.execute(sql, (fund_id,))
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        else:
            return None 
    except Exception as e:
        print(f"❌ 查詢日期失敗: {e}")
        return None
    finally:
        conn.close()

def fetch_data_universal(target_id, start_date, end_date):
    sources = [
        {"type": "境外基金", "url": "https://www.moneydj.com/funddj/bcd/BCDNavList.djbcd", "param_key": "a"},
        {"type": "境內基金", "url": "https://www.moneydj.com/funddj/bcd/tBCDNavList.djbcd", "param_key": "a"},
        {"type": "ETF",    "url": "https://www.moneydj.com/ETF/X/xdjbcd/Basic0003BCD.xdjbcd", "param_key": "etfid"}
    ]
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}

    # 🔴 改用斜線日期 (MoneyDJ 比較吃這套)
    # 輸入進來是 2025-12-24，改成 2025/12/24
    s_date_slash = start_date.replace("-", "/")
    e_date_slash = end_date.replace("-", "/")

    for src in sources:
        try:
            params = {
                src["param_key"]: target_id,
                "d": "" 
            }

            # 🔴 這裡我加上了 print，讓你知道它實際跟 MoneyDJ 要什麼
            if src["type"] == "ETF":
                params["b"] = s_date_slash
                params["c"] = e_date_slash
            else:
                params["B"] = s_date_slash
                params["C"] = e_date_slash

            # print(f"   🐛 [DEBUG] 請求: {src['url']} | 參數: {params}") # 想看細節再打開

            response = requests.get(src["url"], params=params, headers=headers, verify=False)
            
            if response.status_code != 200: continue
            raw_data = response.text
            
            if not raw_data or len(raw_data) < 20: 
                continue 

            parts = raw_data.split(' ')
            if len(parts) < 2: continue

            date_str = parts[0].strip()
            nav_str = parts[1].strip()
            if not date_str or not nav_str: continue

            dates = date_str.split(',')
            navs = nav_str.split(',')
            
            result = []
            min_len = min(len(dates), len(navs))
            
            for i in range(min_len):
                d = dates[i]
                n = navs[i]
                if n.strip() == '': continue
                try:
                    val = float(n)
                    # 轉回資料庫要的格式 YYYY-MM-DD
                    formatted_date = datetime.strptime(d, "%Y%m%d").strftime("%Y-%m-%d")
                    result.append({"date": formatted_date, "nav": val})
                except ValueError:
                    continue
            
            if len(result) > 0:
                print(f"   🎉 命中！識別為 [{src['type']}]，取得 {len(result)} 筆資料")
                return result

        except Exception:
            continue

    return []

def save_navs_to_db(fund_id, data_list):
    if not data_list: return
    conn = get_db_connection()
    if not conn: return

    try:
        # 這裡的 print 還是留著，如果有進來才能再次確認
        dates_to_write = [item['date'] for item in data_list]
        print(f"   🧐 [真相] 程式抓到的日期: {dates_to_write}")

        cursor = conn.cursor()
        insert_data = [(fund_id, item['date'], item['nav']) for item in data_list]
        
        query = """
            INSERT INTO fund_navs (fund_id, nav_date, nav_value)
            VALUES %s
            ON CONFLICT (fund_id, nav_date) DO UPDATE
            SET nav_value = EXCLUDED.nav_value; 
        """
        execute_values(cursor, query, insert_data)
        conn.commit()
        print(f"   💾 成功寫入 {len(data_list)} 筆資料！")
    except Exception as e:
        print(f"❌ 存檔失敗: {e}")
    finally:
        conn.close()

# ==========================================
# 🚀 每日排程主程式
# ==========================================
if __name__ == "__main__":
    print(f"🚀 開始執行每日更新作業 ({datetime.now().strftime('%Y-%m-%d %H:%M')})...\n")
    
    all_funds = get_all_funds_from_db()
    
    if not all_funds:
        print("⚠️ 資料庫裡沒有任何基金")
    
    # 🔴 改用斜線 (MoneyDJ 偏好)
    today_str = datetime.now().strftime("%Y-%m-%d")

    for fund in all_funds:
        f_id = fund[0]
        f_name = fund[1]
        
        print(f"🔍 檢查 [{f_id}] {f_name} ...")
        
        last_date = get_last_date(f_id)
        
        start_date_str = "1990-01-01" 
        
        if last_date:
            next_day = last_date + timedelta(days=1)
            
            # 🔴 註解掉這個檢查！以免因為時區或假未來的問題導致直接跳過
            # if next_day > datetime.now().date():
            #     print(f"   ✅ 已是最新 ({last_date})，跳過。")
            #     print("-" * 40)
            #     continue
                
            start_date_str = next_day.strftime("%Y-%m-%d")
        else:
            print("   ✨ 全新基金，進行全量下載...")

        # 🔴 強制印出我到底要跟 MoneyDJ 要什麼區間
        print(f"   📥 [DEBUG] 準備下載區間: {start_date_str} ~ {today_str}")

        new_data = fetch_data_universal(f_id, start_date_str, today_str)
        
        if new_data:
            save_navs_to_db(f_id, new_data)
        else:
            print("   ⚠️ 無新資料 (MoneyDJ 回傳空，或格式錯誤)")
            
        print("-" * 40)
        
    print("\n✅ 每日更新完畢！")
