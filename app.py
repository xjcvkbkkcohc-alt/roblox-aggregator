import os
import requests
import threading
import time
import logging
from flask import Flask, request, jsonify

# --- Настройка ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# --- Глобальные переменные для статистики ---
# Структура:
# server_data = {
#    'universe_id_1': {
#        'job_id_1': {'count': 10, 'timestamp': 12345},
#        'job_id_2': {'count': 12, 'timestamp': 12346}
#    },
#    'universe_id_2': {
#        'job_id_3': {'count': 5, 'timestamp': 12347}
#    }
# }
server_data = {}
data_lock = threading.Lock() # Замок для безопасной работы со словарем

# --- Настройки интервалов ---
# Как часто сервер отправляет сводку в Discord (5 минут)
AGGREGATE_INTERVAL = 300 
# Через сколько секунд считать игровой сервер "мертвым" (10 минут)
STALE_THRESHOLD = 600 

def keep_alive():
    """Отправляет пинг самому себе, чтобы сервис не "засыпал"."""
    while True:
        # Пауза 10 минут
        time.sleep(600)  
        try:
            render_app_url = os.environ.get('RENDER_EXTERNAL_URL')
            if render_app_url:
                logging.info("Keep-alive: отправляю пинг...")
                # Тайм-аут 15 секунд
                requests.get(render_app_url, timeout=15)
        except requests.RequestException as e:
            logging.error(f"Keep-alive: не удалось отправить пинг: {e}")

def aggregate_and_post_stats():
    """
    Главная функция. Каждые 5 минут собирает статистику и отправляет в Discord.
    """
    # Получаем URL вебхука из настроек Render
    WEBHOOK_URL = os.environ.get('AGGREGATE_WEBHOOK_URL')
    if not WEBHOOK_URL:
        logging.critical("AGGREGATE_WEBHOOK_URL не установлен! Поток статистики не запустится.")
        return

    while True:
        # Ждем 5 минут перед следующим подсчетом
        time.sleep(AGGREGATE_INTERVAL)
        
        logging.info("Агрегатор: Начинаю подсчет статистики...")
        
        total_players = 0
        total_games = 0
        highest_player_count = 0
        active_servers_count = 0
        
        universes_to_remove = []
        current_time = time.time()

        try:
            # Блокируем данные, чтобы безопасно их прочитать и очистить
            with data_lock:
                # Считаем активные игры (Universe ID)
                total_games = len(server_data)
                
                # Проходим по каждой игре (universe)
                for universe_id, jobs in server_data.items():
                    jobs_to_remove = []
                    
                    # Проходим по каждому серверу (job) в этой игре
                    for job_id, data in jobs.items():
                        # Проверяем, не "умер" ли сервер
                        if (current_time - data['timestamp']) > STALE_THRESHOLD:
                            jobs_to_remove.append(job_id)
                        else:
                            # Сервер живой, считаем его
                            player_count = data.get('count', 0)
                            total_players += player_count
                            active_servers_count += 1
                            
                            # Ищем самый населенный сервер
                            if player_count > highest_player_count:
                                highest_player_count = player_count
                    
                    # Удаляем "мертвые" серверы из этой игры
                    for job_id in jobs_to_remove:
                        del server_data[universe_id][job_id]
                        logging.info(f"Агрегатор: Удален мертвый сервер {job_id} из игры {universe_id}")
                
                    # Если в игре не осталось живых серверов, помечаем игру на удаление
                    if not server_data[universe_id]:
                        universes_to_remove.append(universe_id)

                # Удаляем "мертвые" игры
                for universe_id in universes_to_remove:
                    del server_data[universe_id]
                    logging.info(f"Агрегатор: Удалена мертвая игра {universe_id}")

            # --- Отправка статистики в Discord ---
            # Отправляем, только если есть хотя бы один игрок
            if active_servers_count > 0:
                logging.info(f"Агрегатор: Отправка: Игр={total_games}, Игроков={total_players}, Макс.={highest_player_count}")

                # Собираем эмбед, как на твоем скриншоте
                payload = {
                    "embeds": [{
                        "title": "📊 Game Statistics",
                        "color": 11290873, # Фиолетовый (как в твоем старом логгере)
                        "fields": [
                            {
                                "name": "Total Games",
                                "value": f"**{total_games}**",
                                "inline": True
                            },
                            {
                                "name": "Total Players",
                                "value": f"**{total_players}**",
                                "inline": True
                            },
                            {
                                "name": "Highest Player Count",
                                "value": f"**{highest_player_count}**",
                                "inline": True
                            }
                        ],
                        # Используем "Obsidian Serverside" как ты просил
                        "footer": { "text": "Obsidian Serverside" },
                        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                    }]
                }
                
                # Отправляем вебхук
                requests.post(WEBHOOK_URL, json=payload, timeout=10)
            
            else:
                logging.info("Агрегатор: Нет активных серверов, отправка пропущена.")

        except Exception as e:
            logging.error(f"Агрегатор: Ошибка в главном цикле: {e}", exc_info=True)


@app.route('/')
def home():
    """Маршрут для keep-alive и проверки."""
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

        # Простая валидация
        if not all([universe_id, job_id, player_count is not None]):
            logging.warning(f"Heartbeat: Получены неполные данные: {data}")
            return jsonify({"error": "Missing data"}), 400

        current_time = time.time()

        # Безопасно обновляем данные
        with data_lock:
            # Если это первая игра, создаем для нее запись
            if universe_id not in server_data:
                server_data[universe_id] = {}
            
            # Обновляем данные этого конкретного сервера
            server_data[universe_id][job_id] = {
                "count": int(player_count),
                "timestamp": current_time
            }
        
        # logging.info(f"Heartbeat: Получен от {job_id} (Игра: {universe_id}), игроки: {player_count}")
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Heartbeat: Ошибка при обработке: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

# --- Запуск сервера ---
if __name__ == '__main__':
    # Запускаем поток keep-alive
    threading.Thread(target=keep_alive, daemon=True).start()
    
    # Запускаем поток сбора статистики
    threading.Thread(target=aggregate_and_post_stats, daemon=True).start()
    
    # Запускаем Flask-сервер
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
