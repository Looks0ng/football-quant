import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os

# 1. เชื่อมต่อ Supabase
load_dotenv()
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    print("❌ Error: ไม่เจอค่า SUPABASE_URL หรือ SUPABASE_KEY ในไฟล์ .env")
    exit()

supabase: Client = create_client(url, key)

# 2. อ่าน CSV
print("📂 Reading CSV data...")
csv_url = "https://www.football-data.co.uk/mmz4281/2324/E0.csv"
df = pd.read_csv(csv_url)

# 3. เตรียมข้อมูล (Data Transformation)
print("⚙️ Processing & Calculating xG...")

matches_to_insert = []

for index, row in df.iterrows():
    # --- Simple xG Model (สูตรจำลอง xG อย่างง่าย) ---
    # Logic: 
    # - ยิงตรงกรอบ 1 ครั้ง มีโอกาสเป็นประตู 0.3 (30%)
    # - ยิงหลุดกรอบ 1 ครั้ง มีโอกาสเป็นประตู 0.07 (7%)
    # นี่คือโมเดลคณิตศาสตร์เบื้องต้นเพื่อให้มีตัวเลข xG โชว์ใน Dashboard
    
    # คำนวณ Shots Off Target (ยิงหลุดกรอบ)
    h_shots_off = row['HS'] - row['HST']
    a_shots_off = row['AS'] - row['AST']
    
    # คำนวณ xG
    h_xg = (row['HST'] * 0.30) + (h_shots_off * 0.07)
    a_xg = (row['AST'] * 0.30) + (a_shots_off * 0.07)

    match_data = {
        "date": pd.to_datetime(row['Date'], dayfirst=True).strftime('%Y-%m-%d'),
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
        "away_xg": round(a_xg, 2)
    }
    matches_to_insert.append(match_data)

# 4. อัปโหลดทีละ 100 แถว (Batch Insert) ป้องกัน Error
print(f"🚀 Uploading {len(matches_to_insert)} matches to Supabase...")

batch_size = 100
for i in range(0, len(matches_to_insert), batch_size):
    batch = matches_to_insert[i:i+batch_size]
    try:
        data, count = supabase.table("matches").insert(batch).execute()
        print(f"   - Uploaded batch {i} to {i+len(batch)}")
    except Exception as e:
        print(f"❌ Error inserting batch: {e}")

print("✅ Migration Completed! Database is ready.")