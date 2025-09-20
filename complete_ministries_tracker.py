#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robust Ministry Tracker - מעקב דפי שירות עמיד יותר
"""

import requests
import csv
import time
import random
from datetime import datetime
import argparse
import json

class RobustMinistriesTracker:
    def __init__(self):
        self.sessions = []
        
        # יצירת מספר sessions עם הגדרות שונות
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]
        
        for ua in user_agents:
            session = requests.Session()
            session.headers.update({
                'User-Agent': ua,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'he-IL,he;q=0.9,en;q=0.8,en-US;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Referer': 'https://www.gov.il/',
                'Origin': 'https://www.gov.il',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache'
            })
            
            # הוספת SSL verification וטיפול בשגיאות
            session.verify = True
            session.timeout = 45
            
            self.sessions.append(session)
        
        # השפות לבדיקה
        self.languages = ['he', 'ar', 'en', 'es', 'fr', 'ru']
        self.language_names = {
            'he': 'עברית',
            'ar': 'ערבית', 
            'en': 'אנגלית',
            'es': 'ספרדית',
            'fr': 'צרפתית',
            'ru': 'רוסית'
        }
        
        # רשימת המשרדים הספציפיים שביקשת
        self.departments = [
            ("c0d8ba69-e309-4fe5-801f-855971774a90", "רשות המסים בישראל"),
            ("104cb0f4-d65a-4692-b590-94af928c19c0", "משרד הבריאות"),
            ("95b283ad-fc02-40e6-ac6f-8986acac6b86", "רשות האוכלוסין וההגירה"),
            ("48eaee91-0c97-4ef3-b364-50ae52e3b56f", "משרד התחבורה והבטיחות בדרכים"),
            ("85d16bf0-1c3e-486a-97cd-2b07d89e6934", "משרד העבודה"),
            ("8b4e2a06-8ce1-4703-8895-c35be965ef1f", "רשות התאגידים"),
            ("ba3bf87e-6a99-4e24-ae89-2815a450881e", "משרד הכלכלה והתעשייה")
        ]
        
        self.retry_count = 0
        self.max_retries = 3

    def get_random_session(self):
        """בחירת session אקראי"""
        return random.choice(self.sessions)

    def get_services_count(self, dept_id: str, language: str, attempt: int = 0) -> int:
        """קבלת מספר דפי השירותים עבור משרד בשפה מסוימת - עם מנגנון retry מתקדם"""
        
        if attempt >= self.max_retries:
            print(f"    ⚠️ נכשל אחרי {self.max_retries} ניסיונות עבור {dept_id} ({language})")
            return 0
            
        # בחירת session אקראי
        session = self.get_random_session()
        
        # URLs שונים לניסיון
        base_urls = [
            f"https://www.gov.il/{language}/api/GeneralApi/GetModel",
            f"https://gov.il/{language}/api/GeneralApi/GetModel"
        ]
        
        url = base_urls[attempt % len(base_urls)]
        
        params = {
            'ModelName': 'Service',
            'OfficeTaxonomy': dept_id,
            'PageSize': 1,
            'PageNumber': 1
        }
        
        try:
            # עיכוב אקראי כדי להיראות יותר "אנושי"
            time.sleep(random.uniform(0.5, 2.0))
            
            # הוספת headers אקראיים נוספים
            extra_headers = {
                'X-Forwarded-For': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                'X-Real-IP': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                'CF-Connecting-IP': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
            }
            
            response = session.get(url, params=params, headers=extra_headers, timeout=45)
            
            # DEBUG - הדפסה מפורטת
            print(f"    🔍 ניסיון {attempt + 1}: URL={url}, Status={response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    result = data.get('totalResults', 0)
                    if result > 0:
                        print(f"    ✅ מצאתי {result} תוצאות!")
                    return result
                except json.JSONDecodeError:
                    print(f"    ⚠️ תגובה לא תקינה: {response.text[:100]}")
                    return self.get_services_count(dept_id, language, attempt + 1)
                    
            elif response.status_code == 403:
                print(f"    🚫 חסום (403) - מנסה session אחר")
                time.sleep(random.uniform(2, 5))
                return self.get_services_count(dept_id, language, attempt + 1)
                
            elif response.status_code == 429:
                print(f"    ⏳ יותר מדי בקשות (429) - ממתין...")
                time.sleep(random.uniform(5, 10))
                return self.get_services_count(dept_id, language, attempt + 1)
                
            else:
                print(f"    ❌ שגיאה {response.status_code}: {response.text[:100]}")
                return self.get_services_count(dept_id, language, attempt + 1)
                
        except requests.exceptions.Timeout:
            print(f"    ⏰ Timeout - מנסה שוב")
            return self.get_services_count(dept_id, language, attempt + 1)
            
        except requests.exceptions.ConnectionError:
            print(f"    🔌 שגיאת חיבור - מנסה שוב")
            time.sleep(random.uniform(3, 6))
            return self.get_services_count(dept_id, language, attempt + 1)
            
        except Exception as e:
            print(f"    💥 שגיאה כללית: {str(e)}")
            return self.get_services_count(dept_id, language, attempt + 1)

    def scan_department(self, dept_id: str, dept_name: str) -> dict:
        """סריקת משרד יחיד"""
        
        measurement_date = datetime.now().strftime("%B %d, %Y")
        
        dept_data = {
            'תאריך_מדידה': measurement_date,
            'שם_המשרד': dept_name,
            'מזהה_משרד': dept_id
        }
        
        total_services = 0
        
        print(f"  🔍 בודק שפות:")
        for lang in self.languages:
            print(f"    📋 {self.language_names[lang]}...")
            count = self.get_services_count(dept_id, lang)
            dept_data[self.language_names[lang]] = count
            total_services += count
            
        dept_data['סה_כ'] = total_services
        return dept_data

    def scan_all(self, sample_size: int = None) -> list:
        """סריקת כל המשרדים"""
        
        departments_to_scan = self.departments
        
        if sample_size:
            departments_to_scan = departments_to_scan[:sample_size]
            print(f"🎯 בודק דגימה של {sample_size} משרדים")
        else:
            print(f"🔍 בודק את כל {len(departments_to_scan)} המשרדים")
            
        print("-" * 60)
        
        results = []
        
        for i, (dept_id, dept_name) in enumerate(departments_to_scan, 1):
            print(f"\n[{i:2}/{len(departments_to_scan)}] {dept_name}")
            print(f"  ID: {dept_id}")
            
            dept_data = self.scan_department(dept_id, dept_name)
            results.append(dept_data)
            
            if dept_data['סה_כ'] > 0:
                lang_summary = []
                for lang_name in self.language_names.values():
                    count = dept_data[lang_name]
                    if count > 0:
                        lang_summary.append(f"{lang_name}: {count}")
                
                print(f"  ✅ סה\"כ: {dept_data['סה_כ']} דפים")
                print(f"  📊 פרוט: {', '.join(lang_summary)}")
            else:
                print(f"  ❌ לא נמצאו שירותים")
            
            # מנוחה בין משרדים
            if i < len(departments_to_scan):
                wait_time = random.uniform(2, 5)
                print(f"  ⏸️ ממתין {wait_time:.1f} שניות...")
                time.sleep(wait_time)
        
        return results

    def save_to_csv(self, results: list, filename: str = None) -> str:
        """שמירת התוצאות ל-CSV"""
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            filename = f"govil_robust_{timestamp}.csv"
        
        fieldnames = [
            'תאריך_מדידה', 'שם_המשרד', 'עברית', 'ערבית', 
            'אנגלית', 'ספרדית', 'צרפתית', 'רוסית', 'סה_כ', 'מזהה_משרד'
        ]
        
        results.sort(key=lambda x: x['סה_כ'], reverse=True)
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n💾 נשמר: {filename}")
        return filename

    def print_summary(self, results: list):
        """הדפסת סיכום"""
        
        results_with_services = [r for r in results if r['סה_כ'] > 0]
        total_services = sum(r['סה_כ'] for r in results_with_services)
        
        print("\n" + "="*60)
        print("📊 סיכום")
        print("="*60)
        print(f"סה\"ך נבדקו: {len(results)} משרדים")
        print(f"עם שירותים: {len(results_with_services)} משרדים")
        print(f"סה\"ך דפי שירות: {total_services:,}")
        
        if total_services > 0:
            print("\n📈 לפי שפות:")
            for lang_name in self.language_names.values():
                lang_total = sum(r[lang_name] for r in results_with_services)
                percentage = (lang_total / total_services * 100) if total_services > 0 else 0
                print(f"  {lang_name}: {lang_total:,} ({percentage:.1f}%)")
            
            print("\n🏆 משרדים מובילים:")
            for i, r in enumerate(results_with_services[:len(results_with_services)], 1):
                print(f"{i:2}. {r['שם_המשרד']}: {r['סה_כ']} דפים")

def main():
    parser = argparse.ArgumentParser(description="מעקב דפי שירות עמיד - משרדים נבחרים")
    parser.add_argument("--output", "-o", help="קובץ CSV פלט")
    parser.add_argument("--sample", type=int, help="דגימה של N משרדים")
    parser.add_argument("--quiet", "-q", action="store_true", help="פחות פלט")
    
    args = parser.parse_args()
    
    tracker = RobustMinistriesTracker()
    
    try:
        print("🚀 מעקב דפי שירות עמיד - משרדים נבחרים")
        print(f"⏰ {datetime.now().strftime('%d/%m/%Y %H:%M')}")
        print(f"📋 {len(tracker.departments)} משרדים ברשימה")
        print(f"🔄 {len(tracker.sessions)} sessions זמינים")
        print()
        
        results = tracker.scan_all(args.sample)
        
        if results:
            csv_file = tracker.save_to_csv(results, args.output)
            
            if not args.quiet:
                tracker.print_summary(results)
            
            print(f"\n✅ הושלם! קובץ: {csv_file}")
            
        else:
            print("❌ אין תוצאות")
            
    except KeyboardInterrupt:
        print("\n❌ הופסק על ידי המשתמש")
    except Exception as e:
        print(f"❌ שגיאה: {e}")

if __name__ == "__main__":
    main()
