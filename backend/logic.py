import os
import pandas as pd
import numpy as np
from scipy.stats import poisson
from supabase import create_client, Client
from dotenv import load_dotenv

# โหลด Environment Variables
load_dotenv()

class FootballEngine:
    def __init__(self):
        self.df = None
        self.home_stats = None
        self.away_stats = None
        self.avg_home_goals = 0
        self.avg_away_goals = 0
        
        # เชื่อมต่อ Supabase
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("❌ Missing Supabase URL or Key in .env file")
        self.supabase: Client = create_client(url, key)

    def load_data(self):
        print("☁️ Fetching data from Supabase Cloud...")
        try:
            # ดึงข้อมูลทั้งหมดจากตาราง matches
            response = self.supabase.table("matches").select("*").execute()
            
            # แปลงเป็น Pandas DataFrame เพื่อคำนวณง่ายๆ
            self.df = pd.DataFrame(response.data)
            
            # คำนวณค่าพื้นฐานลีก
            self._calculate_metrics()
            print(f"✅ Loaded {len(self.df)} matches successfully!")
        except Exception as e:
            print(f"❌ Error loading data: {e}")

    def _calculate_metrics(self):
        # 1. ค่าเฉลี่ยลีก
        self.avg_home_goals = self.df['home_score'].mean()
        self.avg_away_goals = self.df['away_score'].mean()

        # 2. คำนวณ Strength ของแต่ละทีม (Attack/Defense)
        # Group by Home
        h_stats = self.df.groupby('home_team').agg({
            'home_score': 'mean', 
            'away_score': 'mean',
            'home_xg': 'mean'
        })
        h_stats.rename(columns={'home_score': 'HomeScored', 'away_score': 'HomeConceded', 'home_xg': 'HomeXG'}, inplace=True)
        
        # Group by Away
        a_stats = self.df.groupby('away_team').agg({
            'away_score': 'mean', 
            'home_score': 'mean',
            'away_xg': 'mean'
        })
        a_stats.rename(columns={'away_score': 'AwayScored', 'home_score': 'AwayConceded', 'away_xg': 'AwayXG'}, inplace=True)
        
        self.home_stats = h_stats
        self.away_stats = a_stats

    def get_team_list(self):
        if self.df is None: return []
        return sorted(self.df['home_team'].unique().tolist())

    def get_recent_form(self, team_name, n=5):
        # ดึงฟอร์ม 5 นัดหลังสุด (ทั้งเหย้าและเยือน)
        matches = self.df[
            (self.df['home_team'] == team_name) | (self.df['away_team'] == team_name)
        ].sort_values(by='date', ascending=False).head(n)
        
        goals_scored = 0
        xg_accumulated = 0
        
        for _, row in matches.iterrows():
            if row['home_team'] == team_name:
                goals_scored += row['home_score']
                xg_accumulated += row['home_xg']
            else:
                goals_scored += row['away_score']
                xg_accumulated += row['away_xg']
                
        return {
            "avg_goals": round(goals_scored / n, 2),
            "avg_xg": round(xg_accumulated / n, 2)
        }

    # --- NEW METHOD: คำนวณค่าพลัง 0-100 สำหรับ Radar Chart ---
    def _get_radar_stats(self, team_name):
        # กรองเอาเฉพาะแมตช์ของทีมนี้
        stats = self.df[(self.df['home_team'] == team_name) | (self.df['away_team'] == team_name)]
        
        if len(stats) == 0:
            return {k: 50 for k in ["Attack", "Defense", "Dominance", "Form", "Intensity"]}

        # 1. ATTACK: วัดจากประตูที่ยิงได้เฉลี่ย (เต็ม 100 ที่ 3.0 ประตูต่อนัด)
        goals_for = 0
        for _, row in stats.iterrows():
            if row['home_team'] == team_name: goals_for += row['home_score']
            else: goals_for += row['away_score']
            
        avg_goals = goals_for / len(stats)
        attack_score = min((avg_goals / 3.0) * 100, 99)

        # 2. DEFENSE: วัดจากประตูที่เสียเฉลี่ย (เสียน้อย = คะแนนเยอะ)
        goals_against = 0
        for _, row in stats.iterrows():
            if row['home_team'] == team_name: goals_against += row['away_score']
            else: goals_against += row['home_score']
            
        avg_conceded = goals_against / len(stats)
        defense_score = max((1 - (avg_conceded / 2.5)) * 100, 10) # ถ้าเสียเกิน 2.5 ลูกจะได้คะแนนต่ำ

        # 3. DOMINANCE: วัดจากสัดส่วนโอกาสยิง (Shots Share)
        total_shots_for = 0
        total_shots_against = 0
        for _, row in stats.iterrows():
            if row['home_team'] == team_name:
                total_shots_for += row['home_shots']
                total_shots_against += row['away_shots']
            else:
                total_shots_for += row['away_shots']
                total_shots_against += row['home_shots']
        
        total_shots_match = total_shots_for + total_shots_against
        dominance_score = (total_shots_for / total_shots_match * 100) if total_shots_match > 0 else 50

        # 4. FORM: วัดจาก xG 5 นัดหลัง
        form_data = self.get_recent_form(team_name)
        form_score = min((form_data['avg_xg'] / 2.5) * 100, 99)

        # 5. INTENSITY: วัดจากลูกเตะมุม (Corners) เพื่อดูความกดดัน
        total_corners = 0
        for _, row in stats.iterrows():
            if row['home_team'] == team_name: total_corners += row['home_corners']
            else: total_corners += row['away_corners']
            
        intensity_score = min((total_corners / len(stats) / 8.0) * 100, 95)

        return {
            "Attack": round(attack_score),
            "Defense": round(defense_score),
            "Dominance": round(dominance_score),
            "Form": round(form_score),
            "Intensity": round(intensity_score)
        }

    def predict_match(self, home_team, away_team):
        if self.df is None: self.load_data()
        
        # 1. Base Poisson Calculation
        h_attack = self.home_stats.loc[home_team]['HomeScored'] / self.avg_home_goals
        h_defense = self.home_stats.loc[home_team]['HomeConceded'] / self.avg_away_goals
        a_attack = self.away_stats.loc[away_team]['AwayScored'] / self.avg_away_goals
        a_defense = self.away_stats.loc[away_team]['AwayConceded'] / self.avg_home_goals

        exp_home_goals = h_attack * a_defense * self.avg_home_goals
        exp_away_goals = a_attack * h_defense * self.avg_away_goals

        # 2. Probability Matrix
        max_goals = 6
        probs = np.zeros((max_goals, max_goals))
        for i in range(max_goals):
            for j in range(max_goals):
                probs[i][j] = poisson.pmf(i, exp_home_goals) * poisson.pmf(j, exp_away_goals)
        
        home_win = np.sum(np.tril(probs, -1)) * 100
        draw = np.sum(np.diag(probs)) * 100
        away_win = np.sum(np.triu(probs, 1)) * 100

        # 3. Deep Insight Analysis
        home_form = self.get_recent_form(home_team)
        away_form = self.get_recent_form(away_team)
        
        insight_text = ""
        confidence = "Medium"
        advantage_team = "" 
        
        xg_diff = exp_home_goals - exp_away_goals
        
        if xg_diff > 0.8:
            advantage_team = home_team
            insight_text = f"ระบบมองว่า {home_team} เป็นต่ออย่างมาก ด้วยเกมรุกในบ้านที่ดุดัน และสร้างโอกาส xG คาดหวังถึง {exp_home_goals:.2f} ประตู"
            confidence = "High"
        elif xg_diff < -0.8:
            advantage_team = away_team
            insight_text = f"แม้จะเป็นทีมเยือนแต่ {away_team} เหนือกว่าชัดเจน ระบบคาดการณ์ว่าทีมเยือนจะครองเกมบุกกดดัน และมีโอกาสยิงประตูสูงกว่าเจ้าบ้านมาก"
            confidence = "High"
        elif xg_diff > 0.3:
            advantage_team = home_team
            insight_text = f"{home_team} ได้เปรียบเล็กน้อยจากการเล่นในบ้าน แต่รูปเกมน่าจะสูสี โดยวัดกันที่ความเฉียบคมในจังหวะสุดท้าย"
        elif xg_diff < -0.3:
            advantage_team = away_team
            insight_text = f"{away_team} ดูดีกว่าเล็กน้อยในแง่คุณภาพทีม แต่การมาเยือน {home_team} ไม่ใช่งานง่าย โอกาสออกเสมอมีสูง"
        else:
            advantage_team = "Equal"
            insight_text = f"บอลสามหน้า! สถิติทั้งสองทีมใกล้เคียงกันมาก ระบบมองว่ามีโอกาสออกเสมอสูง หรือเฉือนชนะกันแค่ลูกเดียว"

        # สร้าง Reasons สำหรับ Transparency Report
        reasons = {
            "favorite": advantage_team if advantage_team != "Equal" else "Both Teams",
            "strengths": [],
            "risks": []
        }
        
        if advantage_team == home_team:
            reasons['strengths'] = [
                f"ค่า Attack Strength ในบ้านของ {home_team} สูงกว่าเกณฑ์",
                f"ฟอร์มการเล่น 5 นัดหลังสุดมีค่าเฉลี่ย xG ที่ดี ({home_form['avg_xg']})"
            ]
            reasons['risks'] = [f"ระวังเกมสวนกลับของ {away_team}", "ความกดดันในฐานะเจ้าบ้าน"]
        elif advantage_team == away_team:
             reasons['strengths'] = [
                f"คุณภาพผู้เล่นและค่า xG ของ {away_team} เหนือกว่าเจ้าบ้าน",
                f"เจ้าบ้าน ({home_team}) มีเกมรับที่เสียประตูง่าย"
            ]
             reasons['risks'] = [f"เสียงเชียร์เจ้าบ้านอาจกดดัน {away_team}", "ความล้าจากการเดินทาง"]
        else:
             reasons['strengths'] = ["ทั้งสองทีมมีมาตรฐานใกล้เคียงกัน"]
             reasons['risks'] = ["โอกาสออกเสมอสูงมาก", "รูปเกมอาจจะอึดอัด"]

        return {
            "match": f"{home_team} vs {away_team}",
            "expected_goals": {
                "home": round(exp_home_goals, 2),
                "away": round(exp_away_goals, 2)
            },
            "probabilities": {
                "home_win": round(home_win, 1),
                "draw": round(draw, 1),
                "away_win": round(away_win, 1)
            },
            "stats_comparison": {
                "home_recent_xg": home_form['avg_xg'],
                "away_recent_xg": away_form['avg_xg'],
            },
            "insight": {
                "text": insight_text,
                "confidence": confidence
            },
            "transparency": reasons,
            # --- ส่งข้อมูล Radar Data กลับไปให้ Frontend ---
            "radar_data": {
                "home": self._get_radar_stats(home_team),
                "away": self._get_radar_stats(away_team)
            }
        }