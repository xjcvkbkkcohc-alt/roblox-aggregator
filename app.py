import os
import requests
import threading
import time
import logging
import sys 
from flask import Flask, request, jsonify

# --- Настройка логов ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout # Гарантирует вывод в консоль Render
)

app = Flask(__name__)

# --- Глобальные переменные ---
server_data = {}
data_lock = threading.Lock() 
AGGREGATE_INTERVAL = 300  # 5 минут
STALE_THRESHOLD = 600   # 10 минут

# <--- УДАЛЕНО ---
# Функция keep_alive() была полностью удалена
# --- ---

def aggregate_and_post_stats():
    """
    Каждые 5 минут собирает статистику и отправляет в Discord.
    """
    
    # --- !! ИЗМЕНЕНИЕ !! ---
    #
    # ВСТАВЬ СЮДА СВОЮ РЕАЛЬНУЮ ССЫЛКУ НА ВЕБХУК
    #
    WEBHOOK_URL = "https://discord.com/api/webhooks/1429005345841483776/rxdR0M7CPVXjSE1H4Zw8KvuJ9BIoL85vRRr0bwRVkJ5AL96li0ku2q21xwZOTEXmksju"
    
    # Проверка, что ты вставил свою ссылку
    if "https://discord.com/api/webhooks/1429005345841483776/rxdR0M7CPVXjSE1H4Zw8KvuJ9BIoL85vRRr0bwRVkJ5AL96li0ku2q21xwZOTEXmksju" in WEBHOOK_URL:
        logging.critical("КРИТИЧЕСКАЯ ОШИБКА: Ты не вставил свою ссылку в app.py!")
        print("КРИТИЧЕСКАЯ ОШИБКА: Ты не вставил свою ссылку в app.py!")
        return # Остановить этот поток
        
    logging.info(f"Агрегатор: Поток запущен. URL жестко закодирован.")
    print(f"Агрегатор: Поток запущен. URL жестко закодирован.")
    #
    # --- Конец изменения ---


    while True:
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
                
                    if not server_data[universe_id]:
                        universes_to_remove.append(universe_id)

                for universe_id in universes_to_remove:
                    del server_data[universe_id]

            # --- Отправка статистики в Discord ---
            # Отправляем, только если есть хотя бы один сервер
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

        # Сервер засыпает на 5 минут (300 секунд)
        logging.info(f"Агрегатор: Засыпаю на {AGGREGATE_INTERVAL} секунд...")
        print(f"Агрегатор: Засыпаю на {AGGREGATE_INTERVAL} секунд...")
        time.sleep(AGGREGATE_INTERVAL) 


@app.route('/')
def home():
    """Маршрут для проверки, что сервер жив."""
    return "Obsidian Aggregator Service is running!", 200

@app.route('/heartbeat', methods=['POST'])
def handle_heartbeat():
    """
    Принимает "пульс" от игровых серверов Roblox.
    """
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
        
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Heartbeat: Ошибка при обработке: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

# --- Запуск сервера ---
if __name__ == '__main__':
    # <--- УДАЛЕНО ---
    # Поток keep_alive() больше не запускается
    # threading.Thread(target=keep_alive, daemon=True).start()
    # --- ---
    
    # Запускаем поток сбора статистики
    threading.Thread(target=aggregate_and_post_stats, daemon=True).start()
    
    # Эта переменная окружения (PORT) нужна самому Render, ее убирать нельзя
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
