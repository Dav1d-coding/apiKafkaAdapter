from fastapi import APIRouter, Request
from shema import Message
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json

route = APIRouter()


@route.post('/create_message')
async def send(request: Request):
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        jms = request.headers.get('JMSType')
        route_obj = getRouteData(jms)
        message = await request.body()
        print(f'Sendding message with value: {message}')
        value_json = message
        await producer.send_and_wait(topic=route_obj.get('topic'), value=value_json)
    finally:
        await producer.stop()


def getRouteData(jms_type):
    with open("routes.json") as f:
        routes_data = json.load(f)
        for rd in routes_data.get("routes"):
            if rd.get('JMSType') == jms_type:
                return rd
    return None


async def consume():
    consumer = AIOKafkaConsumer(KAFKA_TOPIC, loop=loop,
                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=KAFKA_CONSUMER_GROUP)
    await consumer.start()
    try:
        async for msg in consumer:
            print(f'Consumer msg: {msg}')
    finally:
        await consumer.stop()

async def consume_cat():
    consumer = AIOKafkaConsumer('NameTopicFirst', loop=loop,
                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=KAFKA_CONSUMER_GROUP)
    await consumer.start()
    try:
        async for msg in consumer:
            print(f'Consumer msg: {msg}')
            print('Sending to cat service')
    finally:
        await consumer.stop()


async def consume_dog():
    consumer = AIOKafkaConsumer('NameSecondTopic', loop=loop,
                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=KAFKA_CONSUMER_GROUP)
    await consumer.start()
    try:
        async for msg in consumer:
            print(f'Consumer msg: {msg}')
            print('Sending to dog service')
    finally:
        await consumer.stop()
