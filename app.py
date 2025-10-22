import os
import requests
import threading
import time
import logging
import sys 
from flask import Flask, request, jsonify

# --- Enhanced logging setup ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
)

logging.getLogger('werkzeug').setLevel(logging.INFO)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# --- Global variables ---
server_data = {}
data_lock = threading.Lock() 
AGGREGATE_INTERVAL = 30   # Check every 30 seconds
STALE_THRESHOLD = 360     # Data older than 6 minutes is stale

def aggregate_and_post_stats():
    """
    Every 30 seconds collects statistics and sends to Discord.
    """
    
    WEBHOOK_URL = "https://discord.com/api/webhooks/1429005345841483776/rxdR0M7CPVXjSE1H4Zw8KvuJ9BIoL85vRRr0bwRVkJ5AL96li0ku2q21xwZOTEXmksju"
    
    if "1429005345841483776" in WEBHOOK_URL:
        print("⚠️ CRITICAL ERROR: You haven't inserted your webhook URL in app.py!", flush=True)
        logging.critical("CRITICAL ERROR: You haven't inserted your webhook URL in app.py!")
        sys.stdout.flush()
        return
        
    print(f"✅ [STARTUP] Aggregator thread started. Checking every {AGGREGATE_INTERVAL}s", flush=True)
    logging.info(f"Aggregator: Thread started. Interval: {AGGREGATE_INTERVAL}s")
    sys.stdout.flush()

    while True:
        print(f"\n{'='*60}", flush=True)
        print(f"[AGGREGATOR] 📊 Starting statistics calculation...", flush=True)
        logging.info("Aggregator: Starting statistics calculation...")
        sys.stdout.flush()
        
        total_players = 0
        total_games = 0
        highest_player_count = 0
        active_servers_count = 0
        
        universes_to_remove = []
        current_time = time.time()

        try:
            with data_lock:
                total_games = len(server_data)
                
                print(f"[AGGREGATOR] 🔍 Current server_data has {total_games} universes", flush=True)
                
                for universe_id, jobs in server_data.items():
                    jobs_to_remove = []
                    for job_id, data in jobs.items():
                        age = current_time - data['timestamp']
                        if age > STALE_THRESHOLD:
                            print(f"[AGGREGATOR] ⏰ Removing stale job {job_id[:8]}... (age: {age:.0f}s)", flush=True)
                            jobs_to_remove.append(job_id)
                        else:
                            player_count = data.get('count', 0)
                            total_players += player_count
                            active_servers_count += 1
                            if player_count > highest_player_count:
                                highest_player_count = player_count
                            print(f"[AGGREGATOR] ✓ Universe {universe_id}, Job {job_id[:8]}..., Players: {player_count}", flush=True)
                    
                    for job_id in jobs_to_remove:
                        del server_data[universe_id][job_id]
                
                    if not server_data[universe_id]:
                        universes_to_remove.append(universe_id)

                for universe_id in universes_to_remove:
                    print(f"[AGGREGATOR] 🗑️ Removing empty universe {universe_id}", flush=True)
                    del server_data[universe_id]

            print(f"[AGGREGATOR] 📈 Summary: Active Servers={active_servers_count}, Total Games={total_games}, Total Players={total_players}, Max={highest_player_count}", flush=True)
            
            # --- Send statistics to Discord ---
            if active_servers_count > 0:
                print(f"[AGGREGATOR] 📤 Sending to Discord...", flush=True)
                logging.info(f"Aggregator: Sending: Games={total_games}, Players={total_players}, Max={highest_player_count}")

                payload = {
                    "embeds": [{
                        "title": "📊 Game Statistics",
                        "color": 11290873, 
                        "fields": [
                            {"name": "Total Games", "value": f"**{total_games}**", "inline": True},
                            {"name": "Total Players", "value": f"**{total_players}**", "inline": True},
                            {"name": "Highest Player Count", "value": f"**{highest_player_count}**", "inline": True}
                        ],
                        "footer": { "text": "Obsidian Serverside" },
                        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                    }]
                }
                response = requests.post(WEBHOOK_URL, json=payload, timeout=10)
                print(f"[AGGREGATOR] ✅ Discord webhook sent! Status: {response.status_code}", flush=True)
                if response.status_code not in [200, 204]:
                    print(f"[AGGREGATOR] ⚠️ Unexpected response: {response.text}", flush=True)
            
            else:
                print(f"[AGGREGATOR] ⏭️ No active servers, skipping Discord send.", flush=True)
                logging.info("Aggregator: No active servers, send skipped.")

        except Exception as e:
            print(f"[AGGREGATOR ERROR] ❌ {e}", flush=True)
            logging.error(f"Aggregator: Error in main loop: {e}", exc_info=True)

        print(f"[AGGREGATOR] 😴 Sleeping for {AGGREGATE_INTERVAL} seconds...", flush=True)
        print(f"{'='*60}\n", flush=True)
        logging.info(f"Aggregator: Sleeping for {AGGREGATE_INTERVAL} seconds...")
        sys.stdout.flush()
        time.sleep(AGGREGATE_INTERVAL) 


@app.route('/')
def home():
    """Route to check server is alive."""
    return "Obsidian Aggregator Service is running!", 200

@app.route('/heartbeat', methods=['POST'])
def handle_heartbeat():
    """
    Receives "heartbeat" from Roblox game servers.
    """
    try:
        data = request.json
        universe_id = data.get('universeId')
        job_id = data.get('jobId')
        player_count = data.get('playerCount')

        if not all([universe_id, job_id, player_count is not None]):
            print(f"[HEARTBEAT] ⚠️ Incomplete data received: {data}", flush=True)
            logging.warning(f"Heartbeat: Incomplete data received: {data}")
            return jsonify({"error": "Missing data"}), 400

        current_time = time.time()

        with data_lock:
            if universe_id not in server_data:
                server_data[universe_id] = {}
            server_data[universe_id][job_id] = {
                "count": int(player_count),
                "timestamp": current_time
            }
        
        print(f"[HEARTBEAT] ✓ Received: Universe={universe_id}, Job={job_id[:8]}..., Players={player_count}", flush=True)
        
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"[HEARTBEAT ERROR] ❌ {e}", flush=True)
        logging.error(f"Heartbeat: Error processing: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

# --- Start server ---
if __name__ == '__main__':
    print("\n" + "="*60, flush=True)
    print("🚀 [STARTUP] Starting Obsidian Aggregator Service", flush=True)
    print("="*60 + "\n", flush=True)
    sys.stdout.flush()
    
    # Start statistics collection thread
    threading.Thread(target=aggregate_and_post_stats, daemon=True).start()
    
    port = int(os.environ.get('PORT', 10000))
    print(f"[STARTUP] 🌐 Starting Flask on port {port}", flush=True)
    sys.stdout.flush()
    
    app.run(host='0.0.0.0', port=port)
