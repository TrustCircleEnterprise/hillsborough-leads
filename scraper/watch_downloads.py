"""
Watches ~/Downloads for new Xleads export files and auto-pushes to GHL
Run once: python3 scraper/watch_downloads.py
Leave it running in the background
"""
import os, sys, time, subprocess
from pathlib import Path
from datetime import datetime

WATCH_DIR = Path.home() / "Downloads"
SCRIPT    = Path.home() / "cobb-leads/scraper/import_xleads.py"
PROCESSED = set()

# Load already processed files
DONE_FILE = Path.home() / "cobb-leads/.processed_imports"
if DONE_FILE.exists():
    PROCESSED = set(DONE_FILE.read_text().splitlines())

def save_processed():
    DONE_FILE.write_text("\n".join(PROCESSED))

def is_xleads_file(path):
    name = path.name.lower()
    return (name.startswith("lpp-export") or "skiptraced" in name) and name.endswith(".csv")

print(f"👀 Watching {WATCH_DIR} for Xleads files...")
print(f"   Any file starting with 'lpp-export' or containing 'skiptraced' will auto-push to GHL")
print(f"   Press Ctrl+C to stop\n")

while True:
    try:
        for f in WATCH_DIR.iterdir():
            if is_xleads_file(f) and str(f) not in PROCESSED:
                age = time.time() - f.stat().st_mtime
                if age < 300:  # only files modified in last 5 minutes
                    print(f"\n🔔 [{datetime.now().strftime('%H:%M:%S')}] New file detected: {f.name}")
                    print(f"   Pushing to GHL...")
                    result = subprocess.run(
                        [sys.executable, str(SCRIPT), str(f)],
                        capture_output=True, text=True
                    )
                    print(result.stdout)
                    if result.stderr:
                        print("Errors:", result.stderr[:200])
                    PROCESSED.add(str(f))
                    save_processed()
        time.sleep(10)
    except KeyboardInterrupt:
        print("\nStopped watching.")
        break
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(10)
