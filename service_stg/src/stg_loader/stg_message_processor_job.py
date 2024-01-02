import json
from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,                 
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        for i in range(self._batch_size):

            # Получите сообщение из Kafka с помощью consume.
            msg_in = self._consumer.consume()

            # Если все сообщения из Kafka обработаны, то consume вернёт None. В этом случае стоит прекратить обработку раньше.
            if msg_in is None:
                break

            # Сохраните сообщение в таблицу, используя _stg_repository.
            object_id = msg_in['object_id']
            object_type = msg_in['object_type']
            sent_dttm = msg_in['sent_dttm']
            payload = json.dumps(msg_in['payload'])

            self._stg_repository.order_events_insert(object_id, object_type, sent_dttm, payload)

            # #получаем информацию о пользователе из Redis
            user_id = msg_in['payload']['user']['id']
            user_info = self._redis.get(user_id)

            # #получаем информацию о ресторане из Redis
            restaurant_id = msg_in['payload']['restaurant']['id']
            restaurant_info = self._redis.get(restaurant_id)

            # создаем выходное сообщение
            payload_out = dict(
                id = object_id,
                date = msg_in['payload']['update_ts'],
                cost = msg_in['payload']['cost'],
                payment = msg_in['payload']['payment'],
                status = msg_in['payload']['final_status'],
                restaurant = {
                    "id": restaurant_id,
                    "name": restaurant_info['name']
                },
                user = {
                    "id": user_id,
                    "name": user_info['name']
                },
                products = []
            )

            # соберём информацию по составу заказа
            for order_item in msg_in['payload']['order_items']:
                item_id = order_item['id']

                for menu_item in restaurant_info['menu']:
                    menu_id = menu_item['_id']

                    if item_id == menu_id:
                        add_item = {
                            "id": item_id,
                            "price": order_item['price'],
                            "quantity": order_item['quantity'],
                            "name": menu_item['name'],
                            "category": menu_item['category']                        
                        }
                        payload_out['products'].append(add_item)
                    
                                  
            msg_out = {
                'object_id': object_id,  
                'object_type': object_type,              
                'payload': payload_out
            }

            # #отправляем выходное сообщение продюсеру
            self._producer.produce(msg_out)


        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
