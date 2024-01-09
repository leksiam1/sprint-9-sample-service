import json
from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.stg_repository import StgRepository

# Получить вложенный елемент из dict с проверкой, если вдруг такого елемента нет(через get)  
def get_nested(data:dict, *args):
    if args and data:
        element  = args[0]
        if element:
            value = data.get(element)
            return value if len(args) == 1 else get_nested(value, *args[1:])   


class StgMessageProcessor:
    JSON_IN_SCHEMA = {
                        "type": "object",
                        "required": ["object_id","object_type","payload","sent_dttm"],
                     }
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
        msg_out = {}

        # Пишем в лог, что джоб был запущен.        
        self._logger.info(f"{datetime.utcnow()}: START")

        for i in range(self._batch_size):
            # Получаем сообщение из Kafka с помощью consume.
            msg_in = self._consumer.consume()
            # Если все сообщения из Kafka обработаны, то consume вернёт None. В этом случае стоит прекратить обработку раньше.
            if msg_in is None:
                break
            #Вставляем JSON в STG
            if msg_in.get('object_type'):
                self._stg_repository.order_events_insert(object_id   = msg_in['object_id'],
                                                            object_type = msg_in['object_type'],
                                                            sent_dttm   = msg_in['sent_dttm'],
                                                            payload     = json.dumps(msg_in['payload']))
                # получаем информацию о пользователе из Redis
                user_id = get_nested(msg_in,"payload","user","id")
                restaurant_id = get_nested(msg_in,"payload","restaurant","id")
                
                # получаем информацию о ресторане из Redis
                user_info = self._redis.get(user_id)
                restaurant_info = self._redis.get(restaurant_id)
                
                #Формируем выходное сообщение            
                msg_out["object_id"]     = msg_in['object_id']
                msg_out["object_type"]   = msg_in['object_type']
                msg_out["payload"]       = {
                                                "id"  : msg_in['object_id'],
                                                "date": get_nested(msg_in,"payload","date"),
                                                "cost": get_nested(msg_in,"payload","cost"),
                                                "payment": get_nested(msg_in,"payload","payment"),
                                                "status" : get_nested(msg_in,"payload","final_status"),
                                                "restaurant": {
                                                                    "id": restaurant_id,
                                                                    "name": restaurant_info.get("name")
                                                                },
                                                "user": {
                                                            "id"   : user_id,
                                                            "name": user_info.get("name")
                                                },
                                                "products": self.get_product_list(get_nested(msg_in,"payload",
                                                                                                      "order_items"),
                                                                                 restaurant_info)
                                                }
                #Отправляем выходное сообщение в kafka
                self._producer.produce(msg_out)
    
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    # Функция для получения категории продукта
    def get_category(self,menu_dict:dict,id_product:str)->str:
        rez = ""
        for item in menu_dict:
            if item["_id"] ==id_product:
                rez = item["category"]
                break        
        return rez    
    
    # Получить список заказанных товаров
    def get_product_list(self,order_items:dict,restaurant_info:dict):
        rez = []
        for item in order_items:
            rez.append({"id":item["id"],
                        "price":item["price"],
                        "quantity":item["quantity"],
                        "name":item["name"],
                        "category":self.get_category(restaurant_info["menu"],item["id"])})
        return rez
