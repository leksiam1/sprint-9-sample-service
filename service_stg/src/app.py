import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor
from stg_loader.repository.stg_repository import StgRepository

batch_size = 100 # Размер батча, обрабатываемого за один запуск. 

app = Flask(__name__)


# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:5000/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()

    # Инициализируем процессор сообщений.
    proc = StgMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        redis=config.redis_client(),
        stg_repository=StgRepository(config.pg_warehouse_db()),
        batch_size=batch_size,
        logger=app.logger
    )

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика(StgMessageProcessor).
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # стартуем Flask-приложение.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
