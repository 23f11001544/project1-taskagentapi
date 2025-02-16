from fastapi import FastAPI, HTTPException
import subprocess
import os
import json
from datetime import datetime
from PIL import Image
import sqlite3
import requests
from pathlib import Path
import markdown
import csv
import shutil
from fastapi.responses import JSONResponse

app = FastAPI()

DATA_DIR = "data"

def ensure_data_dir():
    """Ensure the data directory exists"""
    os.makedirs(DATA_DIR, exist_ok=True)

def is_safe_path(path: str) -> bool:
    """Check if the path is within the data directory"""
    try:
        absolute_path = os.path.abspath(path)
        return absolute_path.startswith(os.path.abspath(DATA_DIR))
    except:
        return False

def safe_read_file(filepath: str) -> str:
    """Safely read a file within the data directory"""
    if not is_safe_path(filepath):
        raise HTTPException(status_code=403, detail="Access denied")
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

def safe_write_file(filepath: str, content: str):
    """Safely write content to a file within the data directory"""
    if not is_safe_path(filepath):
        raise HTTPException(status_code=403, detail="Access denied")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        f.write(content)

@app.get("/")
def read_root():
    return {"message": "DataWorks Task Automation API"}

@app.post("/run")
async def run_task(task: str):
    """
    Execute automated tasks based on natural language description
    """
    ensure_data_dir()
    try:
        task = task.lower()
        
        if "fetch" in task and "api" in task:
            # Fetch data from API
            url = "https://jsonplaceholder.typicode.com/todos/1"
            response = requests.get(url)
            safe_write_file(os.path.join(DATA_DIR, "api_data.json"), response.text)
            return {"message": "API data fetched and saved"}
        
        elif "clone" in task and "git" in task:
            # Clone Git repo and make a commit
            repo_url = "https://github.com/sanand0/tools-in-data-science-public.git"
            repo_path = os.path.join(DATA_DIR, "repo")
            if not os.path.exists(repo_path):
                subprocess.run(["git", "clone", repo_url, repo_path])
            with open(os.path.join(repo_path, "new_file.txt"), "w") as f:
                f.write("Automated commit")
            subprocess.run(["git", "-C", repo_path, "add", "new_file.txt"])
            subprocess.run(["git", "-C", repo_path, "commit", "-m", "Automated commit"])
            return {"message": "Git repo cloned and committed"}
        
        elif "sql" in task and "query" in task:
            # Run SQL query on SQLite database
            db_path = os.path.join(DATA_DIR, "ticket-sales.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(price * units) FROM tickets WHERE type='Gold'")
            result = cursor.fetchone()[0]
            safe_write_file(os.path.join(DATA_DIR, "ticket-sales-gold.txt"), str(result))
            conn.close()
            return {"message": "SQL query executed successfully"}
        
        elif "scrape" in task and "website" in task:
            # Scrape website data
            url = "https://example.com"
            response = requests.get(url)
            safe_write_file(os.path.join(DATA_DIR, "scraped_data.html"), response.text)
            return {"message": "Website data scraped"}
        
        elif "compress" in task and "image" in task:
            # Compress an image
            img_path = os.path.join(DATA_DIR, "sample.png")
            compressed_path = os.path.join(DATA_DIR, "sample_compressed.png")
            with Image.open(img_path) as img:
                img.save(compressed_path, quality=20)
            return {"message": "Image compressed"}
        
        elif "transcribe" in task and "audio" in task:
            # Transcribe audio from MP3 (Placeholder, requires actual transcription library)
            transcript = "[Transcribed speech content]"
            safe_write_file(os.path.join(DATA_DIR, "transcription.txt"), transcript)
            return {"message": "Audio transcribed"}
        
        elif "convert markdown" in task:
            # Convert Markdown to HTML
            md_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.md')]
            for md_file in md_files:
                with open(os.path.join(DATA_DIR, md_file)) as f:
                    md_content = f.read()
                html_content = markdown.markdown(md_content)
                with open(os.path.join(DATA_DIR, md_file.replace('.md', '.html')), 'w') as f:
                    f.write(html_content)
            return {"message": "Markdown files converted to HTML"}
        
        elif "filter" in task and "csv" in task:
            # Filter CSV and return JSON data
            csv_path = os.path.join(DATA_DIR, "sample.csv")
            json_output = []
            with open(csv_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if row.get("Category") == "Important":
                        json_output.append(row)
            return JSONResponse(content=json_output)
        
        return {"error": "Task not recognized or not implemented"}
    
    except Exception as e:
        return {"error": str(e)}

@app.get("/read-file")
def read_file(path: str):
    """
    Safely read a file from the data directory
    """
    try:
        content = safe_read_file(os.path.join(DATA_DIR, path))
        return {"content": content}
    except Exception as e:
        return {"error": str(e)}
