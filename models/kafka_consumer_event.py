from kafka import KafkaConsumer
from models.kafka_producer import KafkaProducer_class
import requests
# from nlp.vosint_v3_event_extraction.processing_events import Extrac_events
import json
import time
import datetime
from core.config import settings
from utils import *

class KafkaConsumer_event_class:
    def __init__(self):
        self.preducer = KafkaProducer_class()
        # self.extract_event = Extrac_events()
        # self.driver = DriverFactory('playwright')
        # self.storage = StorageFactory('hbase')

    def run(self, topic, group_ids="group_id"):
        result = ""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=settings.KAFKA_CONNECT.split(','),
            auto_offset_reset="earliest",
            enable_auto_commit=True,  # Tắt tự động commit offset
            group_id=group_ids,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        messages = consumer.poll(10000, 1)
        consumer.close()
        for tp, messages in messages.items():
            for message in messages:
                # Xử lý message
                message = message.value
                result = self.excute(message)
        return result
    

    def excute(self, message):
        start_date, end_date = get_day_in_week()
        request = requests.post(settings.EXTRACT_API, params={"id":  message['id_new'], "start_date": start_date, "end_date": end_date})
        if not request.ok:
            print("can not extract event")
        # i = 0
        # f = open('/home/ds1/vosint/v-osint-backend/vosint_ingestion/time_log_events.txt','a')
        # f.write(str(i)+ ' '+str(time.time()-start_time)+'\n')
        # f.close()
        # i+=1
        # print("time processing per event", time.time()-start_time)
