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

# 關閉 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 🛠️ 核心功能區
# ==========================================

def get_db_connection():
    if not DB_URI:
        print("❌ 錯誤：找不到 DATABASE_URL，請檢查 .env 檔案！")
        return None
    return psycopg2.connect(DB_URI)

def get_all_funds_from_db():
    """
    從資料庫抓出所有需要監控的基金 ID
    """
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
    """
    查詢這檔基金在資料庫裡「最新」的一筆日期是哪天
    """
    conn = get_db_connection()
    if not conn: return None 
    
    try:
        cursor = conn.cursor()
        sql = "SELECT MAX(nav_date) FROM fund_navs WHERE fund_id = %s;"
        cursor.execute(sql, (fund_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            return result[0] # 回傳 date 物件
        else:
            return None # 代表這檔基金還沒抓過任何資料
            
    except Exception as e:
        print(f"❌ 查詢日期失敗: {e}")
        return None
    finally:
        conn.close()

def fetch_data_universal(target_id, start_date, end_date):
    """
    ✅ 修正版抓取函式：嚴格區分 基金(大寫B,C) 與 ETF(小寫b,c)
    """
    sources = [
        {"type": "境外基金", "url": "https://www.moneydj.com/funddj/bcd/BCDNavList.djbcd", "param_key": "a"},
        {"type": "境內基金", "url": "https://www.moneydj.com/funddj/bcd/tBCDNavList.djbcd", "param_key": "a"},
        {"type": "ETF",    "url": "https://www.moneydj.com/ETF/X/xdjbcd/Basic0003BCD.xdjbcd", "param_key": "etfid"}
    ]
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}

    # print(f"   ↳ 搜尋區間: {start_date} ~ {end_date}")

    for src in sources:
        try:
            # 1. 建立基礎參數 (先不放日期)
            params = {
                src["param_key"]: target_id,
                "d": "" # 防快取
            }

            # 2. 【關鍵修正】根據類型決定日期參數的大小寫
            # 絕對不能混用，否則 MoneyDJ 會錯亂只回傳一年
            if src["type"] == "ETF":
                params["b"] = start_date
                params["c"] = end_date
            else:
                params["B"] = start_date
                params["C"] = end_date

            response = requests.get(src["url"], params=params, headers=headers, verify=False)
            
            if response.status_code != 200: continue
            raw_data = response.text
            
            # 防呆：如果回傳太短，代表這網址沒東西
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
        # 🔴【新增】偵探代碼：印出這一批資料的日期
        dates_to_write = [item['date'] for item in data_list]
        print(f"   🧐 [DEBUG] 準備寫入 Supabase 的日期: {dates_to_write}")
        

        cursor = conn.cursor()
        insert_data = [(fund_id, item['date'], item['nav']) for item in data_list]
        # 加在 save_navs_to_db 函式裡面， cursor = conn.cursor() 的前面
        print(f"   🧐 [真相] 程式抓到的日期是: {[item['date'] for item in data_list]}")
        
        # 使用 UPSERT
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
    
    # 1. 自動從資料庫撈出所有基金清單
    all_funds = get_all_funds_from_db()
    
    if not all_funds:
        print("⚠️ 資料庫裡沒有任何基金 (請先確認 funds 表有資料)")
    
    today_str = datetime.now().strftime("%Y-%m-%d")

    for fund in all_funds:
        f_id = fund[0]
        f_name = fund[1]
        
        print(f"🔍 檢查 [{f_id}] {f_name} ...")
        
        # 2. 找出上次更新到哪一天
        last_date = get_last_date(f_id)
        
        start_date_str = "1990-01-01" # 預設從頭抓
        
        if last_date:
            # 如果有舊資料，從「最後一天的隔天」開始抓
            next_day = last_date + timedelta(days=1)
            
            # 如果隔天已經是將來式(比今天還大)，代表資料很新，不用抓
            if next_day > datetime.now().date():
                print(f"   ✅ 已是最新 ({last_date})，跳過。")
                print("-" * 40)
                continue
                
            start_date_str = next_day.strftime("%Y-%m-%d")
        else:
            print("   ✨ 全新基金，進行全量下載...")

        # 3. 抓取缺漏的區間
        # print(f"   📥 準備下載: {start_date_str} ~ {today_str}")
        new_data = fetch_data_universal(f_id, start_date_str, today_str)
        
        if new_data:
            save_navs_to_db(f_id, new_data)
        else:
            print("   ⚠️ 無新資料 (可能是 MoneyDJ 還沒更新)")
            
        print("-" * 40)
        

    print("\n✅ 每日更新完畢！")

