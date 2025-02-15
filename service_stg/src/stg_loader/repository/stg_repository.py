from datetime import datetime

from lib.pg.pg_connect import PgConnect

class StgRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_events_insert(self,
                            object_id: int,
                            object_type: str,
                            sent_dttm: datetime,
                            payload: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.order_events(
                            object_id, 
                            payload, 
                            object_type, 
                            sent_dttm
                        )
                        VALUES(
                            %(object_id)s, 
                            %(payload)s, 
                            %(object_type)s, 
                            %(sent_dttm)s
                        )
                        on conflict (object_id) do update 
                        set payload = EXCLUDED.payload, 
                            object_type = EXCLUDED.object_type, 
                            sent_dttm = EXCLUDED.sent_dttm
                        ;
                    """,
                    {
                        'object_id': object_id,
                        'object_type': object_type,
                        'sent_dttm': sent_dttm,
                        'payload': payload
                    }
                )