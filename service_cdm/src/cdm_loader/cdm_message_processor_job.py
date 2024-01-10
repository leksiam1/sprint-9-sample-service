from datetime import datetime
from logging import Logger
from uuid import UUID
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:
        
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger        

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg_in = self._consumer.consume()
            if not msg_in:
                break

            user_id = msg_in['user_id']
            products_info = list(zip(
                msg_in['product_id'], 
                msg_in['product_name'], 
                msg_in['category_id'], 
                msg_in['category_name'], 
                msg_in['order_cnt']
            ))

            for product in products_info:
                self._cdm_repository.user_product_counters_insert(user_id, product[0], product[1], product[4])
                self._cdm_repository.user_category_counters_insert(user_id, product[2], product[3])

        self._logger.info(f"{datetime.utcnow()}: FINISH")