import pandas as pd
from supabase import create_client, Client
import os
import sys

# หมายเหตุ: เราจะไม่ใช้ load_dotenv() เพราะบน GitHub Actions เราจะดึง Key จาก System Environment โดยตรง
# แต่เพื่อให้รันในเครื่องได้ด้วย เราจะดักไว้ว่าถ้ามี .env ก็ให้โหลด
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def update_database():
    print("🤖 Starting Smart Update System...")
    
    # 1. เชื่อมต่อ Supabase
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        print("❌ Error: Missing SUPABASE credentials.")
        sys.exit(1) # จบการทำงานแบบแจ้ง Error

    supabase: Client = create_client(url, key)

    # 2. เช็คว่าใน Database มีข้อมูลล่าสุดถึงวันไหน? (Last Checkpoint)
    print("🔍 Checking latest match in database...")
    try:
        # ดึงวันที่มากที่สุด (desc) มา 1 แถว
        response = supabase.table("matches").select("date").order("date", desc=True).limit(1).execute()
        
        last_date = None
        if response.data and len(response.data) > 0:
            last_date = response.data[0]['date']
            print(f"📅 Latest data in DB: {last_date}")
        else:
            print("⚠️ Database is empty. Will import ALL data.")
    except Exception as e:
        print(f"❌ Error checking DB: {e}")
        sys.exit(1)

    # 3. ดึงไฟล์ CSV ล่าสุดจากเว็บ
    print("☁️ Downloading latest CSV from football-data.co.uk...")
    csv_url = "https://www.football-data.co.uk/mmz4281/2324/E0.csv"
    try:
        df = pd.read_csv(csv_url)
        # แปลงวันที่ใน CSV (dd/mm/yyyy) ให้เป็น format มาตรฐาน (yyyy-mm-dd) เพื่อเทียบกันได้
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"❌ Error downloading CSV: {e}")
        sys.exit(1)

    # 4. กรองเอาเฉพาะ "นัดใหม่" (New Matches Only)
    if last_date:
        # เอาเฉพาะแถวที่ Date > last_date
        new_matches = df[df['Date'] > last_date]
    else:
        new_matches = df

    if new_matches.empty:
        print("✅ Database is already up-to-date. No new matches.")
        return

    print(f"🚀 Found {len(new_matches)} new matches. Processing...")

    # 5. Loop คำนวณ xG และเตรียมอัปโหลด
    matches_to_insert = []
    
    for index, row in new_matches.iterrows():
        # คำนวณ xG แบบจำลอง (เหมือนเดิม)
        h_shots_off = row['HS'] - row['HST']
        a_shots_off = row['AS'] - row['AST']
        h_xg = (row['HST'] * 0.30) + (h_shots_off * 0.07)
        a_xg = (row['AST'] * 0.30) + (a_shots_off * 0.07)

        match_data = {
            "date": row['Date'],
            "home_team": row['HomeTeam'],
            "away_team": row['AwayTeam'],
            "home_score": row['FTHG'],
            "away_score": row['FTAG'],
            "home_shots": row['HS'],
            "away_shots": row['AS'],
            "home_shots_target": row['HST'],
            "away_shots_target": row['AST'],
            "home_corners": row['HC'],
            "away_corners": row['AC'],
            "home_xg": round(h_xg, 2),
            "away_xg": round(a_xg, 2),
            # --- เพิ่มส่วนดึงราคา Odds (Bet365) ---
            "odds_home": row.get('B365H', 0), 
            "odds_draw": row.get('B365D', 0),
            "odds_away": row.get('B365A', 0)
        }
        matches_to_insert.append(match_data)

    # 6. Upload ขึ้น Supabase
    try:
        # Supabase API อนุญาตให้ insert ทีละหลาย row ได้
        data, count = supabase.table("matches").insert(matches_to_insert).execute()
        print("✅ Successfully added new matches to database!")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    update_database()