#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets Uploader for Ministry Tracker
מעלה את נתוני המשרדים לגוגל שיטס באופן אוטומטי
"""

import os
import json
import csv
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

class SheetsUploader:
    def __init__(self):
        # הגדרות Google Sheets API
        self.scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # קריאת מפתח השירות מ-environment variable
        service_account_info = json.loads(os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY', '{}'))
        
        if not service_account_info:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY environment variable is required")
        
        # יצירת חיבור
        self.credentials = Credentials.from_service_account_info(
            service_account_info, 
            scopes=self.scopes
        )
        
        self.gc = gspread.authorize(self.credentials)
        
        # מזהה הגיליון
        self.sheet_id = os.environ.get('GOOGLE_SHEET_ID')
        if not self.sheet_id:
            raise ValueError("GOOGLE_SHEET_ID environment variable is required")
    
    def read_csv_data(self, csv_file: str) -> list:
        """קריאת נתונים מקובץ CSV"""
        data = []
        
        with open(csv_file, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # המרת הנתונים לפורמט המתאים לגוגל שיטס
                formatted_row = [
                    row['תאריך_מדידה'],
                    row['שם_המשרד'],
                    int(row['עברית']) if row['עברית'] else 0,
                    int(row['ערבית']) if row['ערבית'] else 0,
                    int(row['אנגלית']) if row['אנגלית'] else 0,
                    int(row['ספרדית']) if row['ספרדית'] else 0,
                    int(row['צרפתית']) if row['צרפתית'] else 0,
                    int(row['רוסית']) if row['רוסית'] else 0,
                    int(row['סה_כ']) if row['סה_כ'] else 0,
                    row['מזהה_משרד']
                ]
                data.append(formatted_row)
        
        return data
    
    def get_last_row(self, worksheet) -> int:
        """מציאת השורה האחרונה עם נתונים"""
        try:
            # קבלת כל הנתונים בעמודה A
            values = worksheet.col_values(1)
            # החזרת מספר השורה האחרונה שלא ריקה
            return len([v for v in values if v.strip()]) if values else 0
        except Exception:
            return 0
    
    def upload_data(self, csv_file: str, worksheet_name: str = 'Sheet1'):
        """העלאת נתונים לגוגל שיטס"""
        
        try:
            # פתיחת הגיליון
            spreadsheet = self.gc.open_by_key(self.sheet_id)
            
            # בחירת גיליון העבודה
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.WorksheetNotFound:
                # יצירת גיליון חדש אם לא קיים
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=10)
                
                # הוספת כותרות
                headers = [
                    'תאריך מדידה', 'שם המשרד', 'עברית', 'ערבית', 'אנגלית', 
                    'ספרדית', 'צרפתית', 'רוסית', 'סה"כ', 'מזהה משרד'
                ]
                worksheet.append_row(headers)
            
            # קריאת נתוני CSV
            new_data = self.read_csv_data(csv_file)
            
            if not new_data:
                print("אין נתונים חדשים להעלאה")
                return
            
            # מציאת השורה האחרונה
            last_row = self.get_last_row(worksheet)
            
            # הוספת הנתונים החדשים
            start_row = last_row + 1
            
            print(f"מעלה {len(new_data)} שורות החל משורה {start_row}")
            
            # הוספת כל השורות בבת אחת (יעיל יותר)
            if new_data:
                # יצירת טווח לעדכון
                end_row = start_row + len(new_data) - 1
                range_name = f'A{start_row}:J{end_row}'
                
                worksheet.update(range_name, new_data, value_input_option='RAW')
                
                print(f"✅ הועלו בהצלחה {len(new_data)} שורות")
                print(f"📊 טווח מעודכן: {range_name}")
            
            # הוספת מידע על העדכון האחרון
            update_info = f"עדכון אחרון: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
            
            # חיפוש או יצירת תא למידע עדכון
            try:
                # עדכון תא מיוחד עם זמן העדכון האחרון
                worksheet.update('L1', update_info)
            except Exception:
                pass
                
        except Exception as e:
            print(f"❌ שגיאה בהעלאת הנתונים: {e}")
            raise
    
    def verify_upload(self, worksheet_name: str = 'Sheet1') -> dict:
        """וידא שהנתונים הועלו בהצלחה"""
        try:
            spreadsheet = self.gc.open_by_key(self.sheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            
            # קבלת מידע בסיסי על הגיליון
            last_row = self.get_last_row(worksheet)
            
            if last_row > 1:  # יש כותרות + נתונים
                # קבלת הערך האחרון בעמודה הראשונה
                last_date = worksheet.cell(last_row, 1).value
                
                return {
                    'success': True,
                    'total_rows': last_row - 1,  # מינוס שורת הכותרות
                    'last_update': last_date
                }
            else:
                return {
                    'success': False,
                    'error': 'לא נמצאו נתונים בגיליון'
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

def main():
    """פונקציה ראשית"""
    
    csv_file = 'ministry_data_temp.csv'
    
    if not os.path.exists(csv_file):
        print(f"❌ קובץ CSV לא נמצא: {csv_file}")
        return
    
    try:
        print("🚀 מתחיל העלאה לגוגל שיטס...")
        
        uploader = SheetsUploader()
        uploader.upload_data(csv_file)
        
        # וידוא העלאה
        verification = uploader.verify_upload()
        
        if verification['success']:
            print(f"✅ הועלה בהצלחה!")
            print(f"📊 סך הכל שורות: {verification['total_rows']}")
            print(f"🕐 עדכון אחרון: {verification['last_update']}")
        else:
            print(f"❌ שגיאה בוידוא: {verification['error']}")
        
        # מחיקת קובץ הזמני
        os.remove(csv_file)
        print(f"🗑️ קובץ זמני נמחק: {csv_file}")
        
    except Exception as e:
        print(f"❌ שגיאה כללית: {e}")
        raise

if __name__ == "__main__":
    main()
