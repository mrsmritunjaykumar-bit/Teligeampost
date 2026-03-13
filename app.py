import os
import time
import threading
import requests
import psycopg2
from psycopg2 import OperationalError
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

# --- Configurations ---
TG_BOT_TOKEN = "8317050457:AAGmtMU36EHToGetT8V8ZReQTgvEAabmwGU"
STORAGE_USER_ID = "6796088344"
CHANNELS = ["-1002867776323", "-1002736849194", "-1003840349067"]
DB_URI = "postgres://avnadmin:AVNS_d9GncXE-Fge9t5p3XlY@pg-7cbbad8-tanyasinghagrawal-62c1.j.aivencloud.com:26734/defaultdb?sslmode=require"
MAX_VIDEO_SIZE = 200 * 1024 * 1024  # 200 MB

# --- Database Functions (with Retry) ---
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
    # Scraped videos table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS scraped_videos (
            id SERIAL PRIMARY KEY,
            video_url TEXT UNIQUE,
            msg_id INTEGER,
            status TEXT DEFAULT 'pending_link', -- pending_link, linked, posted
            bot_link TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Settings table (for default image)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Tables initialized.")

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

# --- Telegram API Helpers (Using Requests/cURL logic) ---
def send_video_to_tg(filepath):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendVideo"
    print(f"[TG] Uploading video to {STORAGE_USER_ID}...")
    with open(filepath, 'rb') as video_file:
        files = {'video': video_file}
        data = {'chat_id': STORAGE_USER_ID}
        response = requests.post(url, data=data, files=files)
    
    res_data = response.json()
    if res_data.get('ok'):
        msg_id = res_data['result']['message_id']
        print(f"[TG] Video uploaded successfully. Msg ID: {msg_id}")
        return msg_id
    else:
        print(f"[TG] Upload failed: {res_data}")
        return None

def send_photo_to_tg(photo_file):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
    files = {'photo': photo_file}
    data = {'chat_id': STORAGE_USER_ID}
    response = requests.post(url, data=data, files=files)
    res_data = response.json()
    if res_data.get('ok'):
        # Get the largest photo id
        file_id = res_data['result']['photo'][-1]['file_id']
        return file_id
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
    # Find last checked number to resume
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT video_url FROM scraped_videos ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    
    current_num = 21 # Start pattern
    if row:
        last_url = row[0]
        try:
            current_num = int(last_url.split('/')[-1].split('.')[0]) + 1
        except:
            pass

    while True:
        folder = (current_num // 1000) * 1000
        video_url = f"https://cdn.desitales2.com/{folder}/{current_num}/{current_num}.mp4"
        
        try:
            # Sirf headers check karo pehle
            head = requests.head(video_url, timeout=10)
            if head.status_code == 200 and 'video' in head.headers.get('Content-Type', ''):
                size = int(head.headers.get('Content-Length', 0))
                
                # Check agar file 200MB se kam hai
                if 0 < size < MAX_VIDEO_SIZE:
                    print(f"[Scraper] Found valid video: {video_url} (Size: {size/(1024*1024):.2f} MB)")
                    
                    # Check duplicate in DB
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT 1 FROM scraped_videos WHERE video_url = %s", (video_url,))
                    if cur.fetchone():
                        print("[Scraper] Already processed, skipping.")
                        conn.close()
                    else:
                        # Download video
                        local_filename = f"temp_vid_{current_num}.mp4"
                        print(f"[Scraper] Downloading {video_url}...")
                        r = requests.get(video_url, stream=True)
                        with open(local_filename, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        # Upload to TG
                        msg_id = send_video_to_tg(local_filename)
                        
                        if msg_id:
                            # Save to DB
                            cur.execute("INSERT INTO scraped_videos (video_url, msg_id, status) VALUES (%s, %s, 'pending_link')", (video_url, msg_id))
                            conn.commit()
                            print(f"[Scraper] Saved to DB. Msg ID: {msg_id}")
                        
                        conn.close()
                        
                        # Delete local file
                        if os.path.exists(local_filename):
                            os.remove(local_filename)
                            print("[Scraper] Local file deleted.")
                else:
                    print(f"[Scraper] File too large or empty: {video_url}")
            else:
                # print(f"[Scraper] Not found/Not video: {video_url}") # Disabled to reduce spam logs
                pass
                
        except Exception as e:
            print(f"[Scraper] Error checking {video_url}: {e}")
        
        current_num += 1
        time.sleep(1) # Sleep to avoid rate limiting from CDN

# --- Background Thread: Auto Poster (30 Mins) ---
def poster_worker():
    print("[Poster] Worker started...")
    last_post_time = 0
    
    while True:
        try:
            current_time = time.time()
            if current_time - last_post_time >= 1800: # 1800 seconds = 30 minutes
                conn = get_db_connection()
                cur = conn.cursor()
                
                # Get oldest linked video that is not yet posted
                cur.execute("SELECT id, bot_link FROM scraped_videos WHERE status = 'linked' ORDER BY id ASC LIMIT 1")
                row = cur.fetchone()
                
                if row:
                    vid_id, bot_link = row
                    default_img_id = get_setting('default_image_id')
                    
                    if default_img_id:
                        print(f"[Poster] Time to post! Sending link {bot_link} to channels...")
                        success_count = 0
                        for channel in CHANNELS:
                            caption = f"🔥 New Video Available!\n\n🔗 Watch Here: {bot_link}"
                            if post_to_channel(channel, default_img_id, caption):
                                success_count += 1
                        
                        if success_count > 0:
                            cur.execute("UPDATE scraped_videos SET status = 'posted' WHERE id = %s", (vid_id,))
                            conn.commit()
                            last_post_time = current_time
                            print(f"[Poster] Successfully posted to {success_count} channels. Waiting 30 mins.")
                        else:
                            print("[Poster] Failed to post to any channel.")
                    else:
                        print("[Poster] Default image not set! Please upload from Web UI.")
                conn.close()
        except Exception as e:
            print(f"[Poster] Error: {e}")
            
        time.sleep(60) # Check every minute

# --- Flask Web & API Endpoints ---
@app.route('/')
def index():
    # Read HTML from index.html file
    with open('index.html', 'r') as f:
        html_content = f.read()
    current_img = get_setting('default_image_id')
    status = f"Current Default Image ID: {current_img}" if current_img else "No default image set."
    return html_content.replace('{{STATUS}}', status)

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
    
    # Run Flask app (binds to port provided by Render)
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
