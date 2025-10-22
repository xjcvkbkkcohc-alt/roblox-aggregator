import os
import requests
import threading
import time
import logging
import sys 
from flask import Flask, request, jsonify

# --- Enhanced logging setup ---
logging.basicConfig(
    level=logging.DEBUG,  # ⬅️ Изменено на DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
)

logging.getLogger('werkzeug').setLevel(logging.INFO)

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

# --- Global variables ---
server_data = {}
data_lock = threading.Lock() 
AGGREGATE_INTERVAL = 30
STALE_THRESHOLD = 360

def aggregate_and_post_stats():
    """
    Every 30 seconds collects statistics and sends to Discord.
    """
    
    WEBHOOK_URL = "https://discord.com/api/webhooks/1429005345841483776/rxdR0M7CPVXjSE1H4Zw8KvuJ9BIoL85vRRr0bwRVkJ5AL96li0ku2q21xwZOTEXmksju"
    
    print(f"\n🔥🔥🔥 AGGREGATOR THREAD STARTING 🔥🔥🔥", flush=True)
    print(f"WEBHOOK_URL = {WEBHOOK_URL}", flush=True)
    print(f"AGGREGATE_INTERVAL = {AGGREGATE_INTERVAL}", flush=True)
    print(f"STALE_THRESHOLD = {STALE_THRESHOLD}", flush=True)
    sys.stdout.flush()
    
    if "1429005345841483776" in WEBHOOK_URL:
        print("❌❌❌ CRITICAL ERROR: DEFAULT WEBHOOK STILL IN CODE!", flush=True)
        return
        
    iteration = 0
    
    while True:
        iteration += 1
        print(f"\n{'#'*80}", flush=True)
        print(f"### ITERATION {iteration} - {time.strftime('%H:%M:%S')} ###", flush=True)
        print(f"{'#'*80}", flush=True)
        sys.stdout.flush()
        
        total_players = 0
        total_games = 0
        highest_player_count = 0
        active_servers_count = 0
        
        universes_to_remove = []
        current_time = time.time()
        
        print(f"[DEBUG] current_time = {current_time}", flush=True)
        print(f"[DEBUG] Acquiring data_lock...", flush=True)

        try:
            with data_lock:
                print(f"[DEBUG] data_lock acquired!", flush=True)
                print(f"[DEBUG] server_data keys: {list(server_data.keys())}", flush=True)
                print(f"[DEBUG] len(server_data) = {len(server_data)}", flush=True)
                
                total_games = len(server_data)
                
                print(f"[AGGREGATOR] 🔍 Total universes in server_data: {total_games}", flush=True)
                
                if total_games == 0:
                    print(f"[DEBUG] ⚠️ server_data is EMPTY! No data to process.", flush=True)
                
                for universe_id, jobs in server_data.items():
                    print(f"\n[DEBUG] Processing universe_id: {universe_id}", flush=True)
                    print(f"[DEBUG] Number of jobs in this universe: {len(jobs)}", flush=True)
                    
                    jobs_to_remove = []
                    for job_id, data in jobs.items():
                        age = current_time - data['timestamp']
                        print(f"[DEBUG]   Job {job_id[:8]}... age={age:.1f}s, count={data.get('count', 0)}", flush=True)
                        
                        if age > STALE_THRESHOLD:
                            print(f"[DEBUG]   ⏰ JOB IS STALE (age {age:.1f}s > threshold {STALE_THRESHOLD}s)", flush=True)
                            jobs_to_remove.append(job_id)
                        else:
                            player_count = data.get('count', 0)
                            total_players += player_count
                            active_servers_count += 1
                            if player_count > highest_player_count:
                                highest_player_count = player_count
                            print(f"[DEBUG]   ✅ JOB IS ACTIVE - Adding {player_count} players", flush=True)
                    
                    print(f"[DEBUG] Jobs to remove from universe {universe_id}: {len(jobs_to_remove)}", flush=True)
                    for job_id in jobs_to_remove:
                        del server_data[universe_id][job_id]
                
                    if not server_data[universe_id]:
                        print(f"[DEBUG] Universe {universe_id} is now empty, marking for removal", flush=True)
                        universes_to_remove.append(universe_id)

                for universe_id in universes_to_remove:
                    print(f"[DEBUG] 🗑️ Removing empty universe {universe_id}", flush=True)
                    del server_data[universe_id]
                
                print(f"[DEBUG] data_lock released!", flush=True)

            print(f"\n{'='*80}", flush=True)
            print(f"[AGGREGATOR] 📊 FINAL STATS:", flush=True)
            print(f"  - Active Servers: {active_servers_count}", flush=True)
            print(f"  - Total Games: {total_games}", flush=True)
            print(f"  - Total Players: {total_players}", flush=True)
            print(f"  - Highest Player Count: {highest_player_count}", flush=True)
            print(f"{'='*80}", flush=True)
            sys.stdout.flush()
            
            # --- Send statistics to Discord ---
            if active_servers_count > 0:
                print(f"\n[AGGREGATOR] ✅ CONDITION MET: active_servers_count > 0", flush=True)
                print(f"[AGGREGATOR] 📤 Preparing Discord payload...", flush=True)

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
                
                print(f"[DEBUG] Payload created: {payload}", flush=True)
                print(f"[DEBUG] Sending POST request to: {WEBHOOK_URL}", flush=True)
                sys.stdout.flush()
                
                try:
                    response = requests.post(WEBHOOK_URL, json=payload, timeout=10)
                    print(f"[AGGREGATOR] 📬 Discord Response Status: {response.status_code}", flush=True)
                    print(f"[DEBUG] Response headers: {response.headers}", flush=True)
                    print(f"[DEBUG] Response text: {response.text}", flush=True)
                    
                    if response.status_code in [200, 204]:
                        print(f"[AGGREGATOR] ✅✅✅ WEBHOOK SENT SUCCESSFULLY! ✅✅✅", flush=True)
                    else:
                        print(f"[AGGREGATOR] ⚠️⚠️⚠️ UNEXPECTED STATUS CODE: {response.status_code}", flush=True)
                        
                except requests.exceptions.Timeout:
                    print(f"[AGGREGATOR] ❌ REQUEST TIMEOUT after 10 seconds!", flush=True)
                except requests.exceptions.ConnectionError as e:
                    print(f"[AGGREGATOR] ❌ CONNECTION ERROR: {e}", flush=True)
                except Exception as e:
                    print(f"[AGGREGATOR] ❌ REQUEST EXCEPTION: {e}", flush=True)
                    
                sys.stdout.flush()
            
            else:
                print(f"\n[AGGREGATOR] ⏭️ CONDITION NOT MET: active_servers_count = {active_servers_count}", flush=True)
                print(f"[AGGREGATOR] ⏭️ NO DATA TO SEND - Skipping Discord webhook", flush=True)

        except Exception as e:
            print(f"\n❌❌❌ AGGREGATOR CRASHED ❌❌❌", flush=True)
            print(f"[AGGREGATOR ERROR] Exception: {e}", flush=True)
            print(f"[DEBUG] Exception type: {type(e)}", flush=True)
            import traceback
            print(f"[DEBUG] Traceback:\n{traceback.format_exc()}", flush=True)
            sys.stdout.flush()

        print(f"\n[AGGREGATOR] 😴 Sleeping for {AGGREGATE_INTERVAL} seconds...", flush=True)
        print(f"{'#'*80}\n", flush=True)
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

        print(f"\n[HEARTBEAT] 📥 Incoming request", flush=True)
        print(f"[DEBUG] Raw data: {data}", flush=True)
        print(f"[DEBUG] universe_id = {universe_id}", flush=True)
        print(f"[DEBUG] job_id = {job_id}", flush=True)
        print(f"[DEBUG] player_count = {player_count}", flush=True)

        if not all([universe_id, job_id, player_count is not None]):
            print(f"[HEARTBEAT] ⚠️ VALIDATION FAILED - Missing data", flush=True)
            return jsonify({"error": "Missing data"}), 400

        current_time = time.time()
        print(f"[DEBUG] current_time = {current_time}", flush=True)
        print(f"[DEBUG] Acquiring data_lock for write...", flush=True)

        with data_lock:
            print(f"[DEBUG] data_lock acquired for write!", flush=True)
            
            if universe_id not in server_data:
                print(f"[DEBUG] Creating new universe entry: {universe_id}", flush=True)
                server_data[universe_id] = {}
                
            server_data[universe_id][job_id] = {
                "count": int(player_count),
                "timestamp": current_time
            }
            
            print(f"[DEBUG] Data saved! server_data now has {len(server_data)} universes", flush=True)
            print(f"[DEBUG] Universe {universe_id} has {len(server_data[universe_id])} jobs", flush=True)
            print(f"[DEBUG] data_lock released!", flush=True)
        
        print(f"[HEARTBEAT] ✅ Success: Universe={universe_id}, Job={job_id[:8]}..., Players={player_count}", flush=True)
        sys.stdout.flush()
        
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"\n❌❌❌ HEARTBEAT CRASHED ❌❌❌", flush=True)
        print(f"[HEARTBEAT ERROR] Exception: {e}", flush=True)
        import traceback
        print(f"[DEBUG] Traceback:\n{traceback.format_exc()}", flush=True)
        sys.stdout.flush()
        return jsonify({"error": "Internal server error"}), 500

# --- Start server ---
if __name__ == '__main__':
    print("\n" + "="*80, flush=True)
    print("🚀🚀🚀 STARTING OBSIDIAN AGGREGATOR SERVICE 🚀🚀🚀", flush=True)
    print("="*80 + "\n", flush=True)
    sys.stdout.flush()
    
    print(f"[STARTUP] Creating aggregator thread...", flush=True)
    aggregator_thread = threading.Thread(target=aggregate_and_post_stats, daemon=True)
    print(f"[STARTUP] Starting aggregator thread...", flush=True)
    aggregator_thread.start()
    print(f"[STARTUP] Aggregator thread started! Thread alive: {aggregator_thread.is_alive()}", flush=True)
    sys.stdout.flush()
    
    port = int(os.environ.get('PORT', 10000))
    print(f"[STARTUP] 🌐 Starting Flask on port {port}", flush=True)
    sys.stdout.flush()
    
    app.run(host='0.0.0.0', port=port)
