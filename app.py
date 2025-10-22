import os
import requests
import threading
import time
import logging
import sys # <--- ДОБАВЛЕНО
from flask import Flask, request, jsonify

# --- Настройка ---
# ДОБАВЛЕНО 'stream=sys.stdout', чтобы логи 100% шли в консоль Render
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

app = Flask(__name__)

# --- Глобальные переменные (без изменений) ---
server_data = {}
data_lock = threading.Lock() 

# --- Настройки интервалов (без изменений) ---
AGGREGATE_INTERVAL = 300 
STALE_THRESHOLD = 600 

def keep_alive():
    while True:
        time.sleep(600)  
        try:
            render_app_url = os.environ.get('RENDER_EXTERNAL_URL')
            if render_app_url:
                logging.info("Keep-alive: отправляю пинг...")
                print("Keep-alive: отправляю пинг...") # <--- ДОБАВЛЕНО
                requests.get(render_app_url, timeout=15)
        except requests.RequestException as e:
            logging.error(f"Keep-alive: не удалось отправить пинг: {e}")
            print(f"Keep-alive: не удалось отправить пинг: {e}") # <--- ДОБАВЛЕНО

def aggregate_and_post_stats():
    """
    Главная функция. Каждые 5 минут собирает статистику и отправляет в Discord.
    """
    WEBHOOK_URL = os.environ.get('AGGREGATE_WEBHOOK_URL')
    if not WEBHOOK_URL:
        # ЭТО ВАЖНО!
        logging.critical("КРИТИЧЕСКАЯ ОШИБКА: 'AGGREGATE_WEBHOOK_URL' не установлена! Поток статистики не запустится.")
        print("КРИТИЧЕСКАЯ ОШИБКА: 'AGGREGATE_WEBHOOK_URL' не установлена! Поток статистики не запустится.")
        return # <-- Поток умирает здесь, если URL не найден

    logging.info("Агрегатор: Поток запущен. Первая проверка... (URL вебхука найден)")
    print("Агрегатор: Поток запущен. Первая проверка... (URL вебхука найден)")

    while True:
        # --- ЛОГИКА ВЫПОЛНЯЕТСЯ СНАЧАЛА ---
        logging.info("Агрегатор: Начинаю подсчет статистики...")
        print("Агрегатор: Начинаю подсчет статистики...")
        
        total_players = 0
        total_games = 0
        highest_player_count = 0
        active_servers_count = 0
        
        universes_to_remove = []
        current_time = time.time()

        try:
            with data_lock:
                total_games = len(server_data)
                
                for universe_id, jobs in server_data.items():
                    jobs_to_remove = []
                    for job_id, data in jobs.items():
                        if (current_time - data['timestamp']) > STALE_THRESHOLD:
                            jobs_to_remove.append(job_id)
                        else:
                            player_count = data.get('count', 0)
                            total_players += player_count
                            active_servers_count += 1
                            if player_count > highest_player_count:
                                highest_player_count = player_count
                    
                    for job_id in jobs_to_remove:
                        del server_data[universe_id][job_id]
                        logging.info(f"Агрегатор: Удален мертвый сервер {job_id}")
                
                    if not server_data[universe_id]:
                        universes_to_remove.append(universe_id)

                for universe_id in universes_to_remove:
                    del server_data[universe_id]
                    logging.info(f"Агрегатор: Удалена мертвая игра {universe_id}")

            # --- Отправка статистики в Discord ---
            if active_servers_count > 0:
                logging.info(f"Агрегатор: Отправка: Игр={total_games}, Игроков={total_players}, Макс.={highest_player_count}")
                print(f"Агрегатор: Отправка: Игр={total_games}, Игроков={total_players}, Макс.={highest_player_count}")

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
                requests.post(WEBHOOK_URL, json=payload, timeout=10)
            
            else:
                logging.info("Агрегатор: Нет активных серверов, отправка пропущена.")
                print("Агрегатор: Нет активных серверов, отправка пропущена.")

        except Exception as e:
            logging.error(f"Агрегатор: Ошибка в главном цикле: {e}", exc_info=True)
            print(f"Агрегатор: Ошибка в главном цикле: {e}")

        # --- ТЕПЕРЬ СЕРВЕР ЗАСЫПАЕТ ---
        # time.sleep() был перенесен ИЗ НАЧАЛА в КОНЕЦ цикла
        logging.info(f"Агрегатор: Засыпаю на {AGGREGATE_INTERVAL} секунд...")
        print(f"Агрегатор: Засыпаю на {AGGREGATE_INTERVAL} секунд...")
        time.sleep(AGGREGATE_INTERVAL) 


@app.route('/')
def home():
    return "Obsidian Aggregator Service is running!", 200

@app.route('/heartbeat', methods=['POST'])
def handle_heartbeat():
    try:
        data = request.json
        universe_id = data.get('universeId')
        job_id = data.get('jobId')
        player_count = data.get('playerCount')

        if not all([universe_id, job_id, player_count is not None]):
            logging.warning(f"Heartbeat: Получены неполные данные: {data}")
            return jsonify({"error": "Missing data"}), 400

        current_time = time.time()

        with data_lock:
            if universe_id not in server_data:
                server_data[universe_id] = {}
            
            server_data[universe_id][job_id] = {
                "count": int(player_count),
                "timestamp": current_time
            }
        
        # Раскомментируй, если хочешь видеть КАЖДЫЙ пинг в логах (будет спам)
        # logging.info(f"Heartbeat: Пинг от {job_id}, игроки: {player_count}")
        
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Heartbeat: Ошибка при обработке: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

# --- Запуск сервера (без изменений) ---
if __name__ == '__main__':
    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=aggregate_and_post_stats, daemon=True).start()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
