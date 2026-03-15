import os
import time
import threading
import requests
import psycopg2
from psycopg2 import OperationalError
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- Configurations ---
TG_BOT_TOKEN = "8317050457:AAGmtMU36EHToGetT8V8ZReQTgvEAabmwGU"
STORAGE_USER_ID = "6796088344"
CHANNELS = ["-1002867776323", "-1002736849194", "-1003840349067"]
DB_URI = "postgres://avnadmin:AVNS_d9GncXE-Fge9t5p3XlY@pg-7cbbad8-tanyasinghagrawal-62c1.j.aivencloud.com:26734/defaultdb?sslmode=require"
MAX_VIDEO_SIZE = 49 * 1024 * 1024  # 200 MB
MAX_SCAN_LIMIT = 3000

# --- Database Functions ---
def get_db_connection(retries=5, delay=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(DB_URI)
            return conn
        except OperationalError as e:
            print(f"[DB] Connection failed, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Database connection failed after retries.")

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS scraped_videos (
            id SERIAL PRIMARY KEY,
            video_url TEXT UNIQUE,
            msg_id INTEGER,
            status TEXT DEFAULT 'pending_link',
            bot_link TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()

def get_setting(key):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT value FROM bot_settings WHERE key = %s", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def set_setting(key, value):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))
    conn.commit()
    conn.close()

# --- Telegram API Helpers ---
def send_status_to_tg(message):
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        data = {'chat_id': STORAGE_USER_ID, 'text': message}
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print(f"[TG Status Error] {e}")

def send_video_to_tg(filepath):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendVideo"
    with open(filepath, 'rb') as video_file:
        files = {'video': video_file}
        data = {'chat_id': STORAGE_USER_ID}
        response = requests.post(url, data=data, files=files)
    
    res_data = response.json()
    if res_data.get('ok'):
        return res_data['result']['message_id']
    return None

def send_photo_to_tg(photo_file):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
    files = {'photo': photo_file}
    data = {'chat_id': STORAGE_USER_ID}
    response = requests.post(url, data=data, files=files)
    res_data = response.json()
    if res_data.get('ok'):
        return res_data['result']['photo'][-1]['file_id']
    return None

def post_to_channel(channel_id, file_id, caption):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
    data = {
        'chat_id': channel_id,
        'photo': file_id,
        'caption': caption
    }
    response = requests.post(url, data=data)
    return response.json().get('ok', False)

# --- Background Thread: Video Scraper ---
def scraper_worker():
    print("[Scraper] Worker started...")
    send_status_to_tg("🚀 Render Scraper Bot Start ho gaya hai! Checking backlogs...")
    
    # Check last checked number from settings
    current_num_str = get_setting('current_num')
    current_num = int(current_num_str) if current_num_str else 21

    while True:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            # 1. Backlog Check: Agar koi video post hone ke liye pending hai, toh wait karo
            cur.execute("SELECT COUNT(*) FROM scraped_videos WHERE status IN ('pending_link', 'linked')")
            backlog = cur.fetchone()[0]
            
            if backlog > 0:
                conn.close()
                print(f"[Scraper] Backlog is {backlog}. Waiting for poster to clear...")
                time.sleep(60) # Wait 1 minute and check again
                continue
            
            # 2. Reached Limit Check
            if current_num > MAX_SCAN_LIMIT:
                current_num = 21
                set_setting('current_num', str(current_num))
                send_status_to_tg("🔄 3000 videos scan ho gaye! Ab 21 se wapas purani videos recycle (re-post) karunga.")
            
            folder = (current_num // 1000) * 1000
            video_url = f"https://cdn.desitales2.com/{folder}/{current_num}/{current_num}.mp4"
            
            # 3. Check if URL already in DB
            cur.execute("SELECT id, bot_link FROM scraped_videos WHERE video_url = %s", (video_url,))
            row = cur.fetchone()
            
            if row:
                db_id, bot_link = row
                if bot_link:
                    # Recycle existing video without downloading
                    cur.execute("UPDATE scraped_videos SET status = 'linked' WHERE id = %s", (db_id,))
                    conn.commit()
                    conn.close()
                    send_status_to_tg(f"♻️ Purani video mili: {current_num}. Bina download kiye queue me daal di. Agle 30 min me post hogi.")
                    current_num += 1
                    set_setting('current_num', str(current_num))
                    continue
                else:
                    # DB me hai par link nahi hai (failed earlier), treat as pending
                    cur.execute("UPDATE scraped_videos SET status = 'pending_link' WHERE id = %s", (db_id,))
                    conn.commit()
                    conn.close()
                    current_num += 1
                    set_setting('current_num', str(current_num))
                    continue

            conn.close() # Close DB connection before heavy network tasks

            # 4. URL not in DB, Check and Download
            head = requests.head(video_url, timeout=10)
            if head.status_code == 200 and 'video' in head.headers.get('Content-Type', ''):
                size = int(head.headers.get('Content-Length', 0))
                
                if 0 < size < MAX_VIDEO_SIZE:
                    send_status_to_tg(f"⏳ Nayi video mili: {video_url} ({size/(1024*1024):.2f} MB). Downloading...")
                    
                    local_filename = f"temp_vid_{current_num}.mp4"
                    r = requests.get(video_url, stream=True)
                    with open(local_filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    msg_id = send_video_to_tg(local_filename)
                    
                    if msg_id:
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("INSERT INTO scraped_videos (video_url, msg_id, status) VALUES (%s, %s, 'pending_link')", (video_url, msg_id))
                        conn.commit()
                        conn.close()
                        send_status_to_tg(f"✅ Video Downloaded aur Telegram par bhej di gayi (Msg ID: {msg_id}). Local DB Sync ka wait kar raha hoon.")
                    else:
                        send_status_to_tg(f"❌ Telegram par video bhejne me error aaya: {video_url}")
                    
                    if os.path.exists(local_filename):
                        os.remove(local_filename)
            
            # Move to next number
            current_num += 1
            set_setting('current_num', str(current_num))
            time.sleep(1) # Prevent spamming CDN
            
        except Exception as e:
            print(f"[Scraper Error] {e}")
            time.sleep(5)

# --- Background Thread: Auto Poster (30 Mins) ---
def poster_worker():
    print("[Poster] Worker started...")
    last_post_time = 0
    
    while True:
        try:
            current_time = time.time()
            if current_time - last_post_time >= 1800: # 30 minutes
                conn = get_db_connection()
                cur = conn.cursor()
                
                cur.execute("SELECT id, bot_link FROM scraped_videos WHERE status = 'linked' ORDER BY id ASC LIMIT 1")
                row = cur.fetchone()
                
                if row:
                    vid_id, bot_link = row
                    default_img_id = get_setting('default_image_id')
                    
                    if default_img_id:
                        success_count = 0
                        for channel in CHANNELS:
                            caption = f"🔥 New Video Available!\n\n🔗 Watch Here: {bot_link}"
                            if post_to_channel(channel, default_img_id, caption):
                                success_count += 1
                        
                        if success_count > 0:
                            cur.execute("UPDATE scraped_videos SET status = 'posted' WHERE id = %s", (vid_id,))
                            conn.commit()
                            last_post_time = current_time
                            send_status_to_tg(f"📢 Video {success_count} channels me post ho gayi!\nLink: {bot_link}\n\nAgli post 30 minute baad.")
                        else:
                            send_status_to_tg("⚠️ Channels me post karne me error aa raha hai. Check if bot is admin.")
                    else:
                        send_status_to_tg("⚠️ Default Image set nahi hai! Web UI se image upload karein taaki posting shuru ho sake.")
                conn.close()
        except Exception as e:
            print(f"[Poster Error] {e}")
            
        time.sleep(60) # Check every minute

# --- Flask Web & API Endpoints ---
@app.route('/')
def index():
    try:
        with open('index.html', 'r') as f:
            html_content = f.read()
        current_img = get_setting('default_image_id')
        status = f"Current Default Image ID: {current_img}" if current_img else "No default image set."
        return html_content.replace('{{STATUS}}', status)
    except Exception as e:
        return f"Error loading index: {e}"

@app.route('/upload_image', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        return "No image uploaded", 400
    file = request.files['image']
    if file.filename == '':
        return "Empty file", 400
    
    file_id = send_photo_to_tg(file.read())
    if file_id:
        set_setting('default_image_id', file_id)
        send_status_to_tg("🖼️ Nayi default image successfully update ho gayi hai!")
        return f"Image uploaded to Telegram successfully! File ID saved: {file_id}. <br><a href='/'>Go Back</a>"
    else:
        return "Failed to upload image to Telegram.", 500

@app.route('/api/get_pending', methods=['GET'])
def get_pending():
    try:
        conn = get_db_connection(retries=1)
        cur = conn.cursor()
        cur.execute("SELECT id, msg_id FROM scraped_videos WHERE status = 'pending_link' ORDER BY id ASC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        
        if row:
            return jsonify({'success': True, 'id': row[0], 'msg_id': row[1]})
        return jsonify({'success': False, 'message': 'No pending videos'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/submit_link', methods=['POST'])
def submit_link():
    data = request.json
    db_id = data.get('id')
    bot_link = data.get('link')
    
    if db_id and bot_link:
        try:
            conn = get_db_connection(retries=2)
            cur = conn.cursor()
            cur.execute("UPDATE scraped_videos SET status = 'linked', bot_link = %s WHERE id = %s", (bot_link, db_id))
            conn.commit()
            conn.close()
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    return jsonify({'success': False, 'error': 'Invalid data'})

if __name__ == '__main__':
    init_db()
    
    # Start background threads
    threading.Thread(target=scraper_worker, daemon=True).start()
    threading.Thread(target=poster_worker, daemon=True).start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
