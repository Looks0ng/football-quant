from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from logic import FootballEngine
from contextlib import asynccontextmanager

# --- Life Span Manager ---
# ฟังก์ชันนี้จะทำงานตอน Server เริ่ม และ จบ
# เราจะโหลดข้อมูลแค่ครั้งเดียวตอนเริ่ม Server (จะได้ไม่หน่วงตอน User ใช้งาน)
engine = FootballEngine()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: โหลดข้อมูล
    engine.load_data()
    yield
    # Shutdown: (ถ้ามีอะไรต้องปิด ให้ใส่ตรงนี้)
    pass

app = FastAPI(lifespan=lifespan)

# --- 2. เพิ่มส่วนตั้งค่า CORS ตรงนี้ (ใส่ใต้ app = FastAPI) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ยอมให้ทุกเว็บเรียกใช้ API นี้ได้ (ตอน Dev ใช้ * ไปก่อน)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# --- Pydantic Model (Data Validation) ---
# กำหนดหน้าตาข้อมูลที่ User ต้องส่งมา
class MatchRequest(BaseModel):
    home_team: str
    away_team: str

# --- Endpoints ---

@app.get("/")
def read_root():
    return {"status": "Football Quant API is running 🚀"}

@app.get("/teams")
def get_teams():
    # API สำหรับดึงรายชื่อทีมไปทำ Dropdown
    return {"teams": engine.get_team_list()}

@app.post("/predict")
def predict(request: MatchRequest):
    # รับชื่อทีม -> ส่งให้ Engine คำนวณ -> คืนผลลัพธ์
    result = engine.predict_match(request.home_team, request.away_team)
    
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
        
    return result