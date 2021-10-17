import logging
from datetime import datetime, timedelta
from typing import Dict, List
import json
from app import db
from app.udaconnect.models import Person
from app.udaconnect.schemas import PersonSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text
from kafka import KafkaConsumer, KafkaProducer
from flask import g, request, jsonify
from flask_cors import CORS


# Set up a Kafka producer
TOPIC_NAME = 'person'
KAFKA_SERVER = '34.125.83.32:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-api-person")


class PersonService:
    @staticmethod
    def create(person: Dict) -> Person:
        new_person = Person()
        new_person.first_name = person["first_name"]
        new_person.last_name = person["last_name"]
        new_person.company_name = person["company_name"]
        # Turn order_data into a binary string for Kafka
        kafka_data = json.dumps(str(new_person)).encode()
        # Kafka producer has already been set up in Flask context
        kafka_producer = producer
        kafka_producer.send("person", kafka_data)
        print('Kafka Request Successful')
        db.session.add(new_person)
        db.session.commit()

        return new_person

    @staticmethod
    def retrieve(person_id: int) -> Person:
        person = db.session.query(Person).get(person_id)
        return person

    @staticmethod
    def retrieve_all() -> List[Person]:
        return db.session.query(Person).all()
