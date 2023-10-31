from kafka import KafkaConsumer
from models.kafka_producer import KafkaProducer_class
import requests
# from nlp.vosint_v3_event_extraction.processing_events import Extrac_events
import json
import time
import datetime
from core.config import settings
from utils import *
from models.mongorepository import MongoRepository

class KafkaConsumer_event_class:
    def __init__(self):
        self.preducer = KafkaProducer_class()

    def run(self, topic, group_ids="group_id"):
        result = ""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[settings.KAFKA_CONNECT],
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
    
    def summarize(self, lang: str = "", title: str = "", paras: str = "", k: float = 0.4):
        try:
            request = requests.post(
                settings.SUMMARIZE_API,
                data=json.dumps(
                    {
                        "lang": lang,
                        "title": title,
                        "paras": paras,
                        "k": k,
                        "description": "",
                    }
                ),
            )
            if request.status_code != 200:
                raise Exception("Summarize failed")
            data = str(request.json().get("Extractive summarization"))
            return data
        except:
            return ""

    def summarize_all_level(self, lang:str = "", title:str = "", paras:str= "", ks:list[float]=[0.2,0.4,0.6,0.8]):
        result = {}
        for k in ks:
            result[f"s{int(k*100)}"] = self.summarize(lang, title, paras, k)
        return result

    def excute(self, message):
        start_date, end_date = get_day_in_week()
        request = requests.post(settings.EXTRACT_API, params={"id":  message['id_new'], "start_date": start_date, "end_date": end_date})
        if not request.ok:
            print("can not extract event")
        try:
            data = request.json()
            event_id = data.get("id_new")
            if event_id != None:
                event = MongoRepository().get_one("events", {"_id": event_id})
                if event != None:
                    news_id = event.get("new_list")[0]
                    news = MongoRepository().get_one("News", {"_id": news_id})
                    lang = news.get("source_language")
                    summ = self.summarize_all_level(lang, event["event_name"], event["event_content"])
                    MongoRepository().update_many("events", {"_id": event.get("_id")}, {"$set": {"data:summaries": summ}})
        except Exception as e:
            print(e)
                
