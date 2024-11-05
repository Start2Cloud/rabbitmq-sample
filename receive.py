#!/usr/bin/env python
import time
import random
import pika
import json

# Default test host in GTM is 172.16.1.57
# Web UI http://172.16.1.57:15672/#/
RABBITMQ_HOST = '172.16.1.57'
# default port is 5672
RABBITMQ_PORT = 5672

# Default user and password is guest and you can do everything for dev
#RABBITMQ_USER = 'guest'
#RABBITMQ_PWD  = 'guest'

# gient_dev_sender is only send to Exange giant.direct for permission sample
RABBITMQ_USER = 'gient_dev_sender'
RABBITMQ_PWD  = 'gient_dev_sender'

# default virtual host 
RABBITMQ_VIRTUALHOST = '/'

# Message acknowledgment
# 為了要確保message一定能收到(ex. consumer dies), ack設定在worker必須回傳delivery_tag,
# 給server(收到後server才會從queue中刪除此訊息),否則server會重新發出此message
# ---------- no_ack ---------------------------
# def callback(ch, method, properties, body):
#     print(" [x] Received %r" % body)
#     time.sleep(body.count(b'.'))
#     print(" [x] Done")
#
#
# channel.basic_consume(callback,
#                       queue='hello',
#                       no_ack=True)  # 預設是有ack
# ---------------------------------------------
def callback(ch, method, properties, body):
    #print(body.decode())
    # body = b'Hello Word!!!Sun Jul 28 16:12:36 2024'
    # body.decode() = Hello Word!!!Sun Jul 28 16:12:36 2024
    print(" [!] Received %r" % (body.decode()))

    try:
        # Attempt to decode the message as JSON
        message = json.loads(body.decode('utf-8'))
        print(" [!] Received JSON message: %r" % message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.JSONDecodeError:
        # If it's not valid JSON, print the raw message
        print(" [!] Received non-JSON message: %r" % body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
   
    """
    print(" >>>>> Do something for " + body.decode())
    if body.decode() == 'Andy test':
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(" [O] Done \n")
    else:
        ch.basic_reject(delivery_tag = method.delivery_tag, requeue=False)
        print(" [O] Done \n")
    """
    # time.sleep(1)

    """
    if random.random() < 0.5:
        #如果正確處理，回傳delivery_tag，queue 會被移除
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(" [O] Done \n")
    else:
        #如果處理異常，回傳 reject，資料會被丟到 dead queue
        ch.basic_reject(delivery_tag = method.delivery_tag, requeue=False)
        print(" [X] Rejected, send to [giant.dead] \n")
    """


while True:
    try:
        # create connection
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        channel = connection.channel()
        # queue_name = 'giant.direct.queue-3'
        queue_name = 'FromAIP2SAP_DN_TOAIP2_WMS'
        # Check queue
        channel.queue_declare(queue_name, durable = True,
            arguments={
                #"x-dead-letter-exchange" : "giant.dead",
                #"x-queue-type" : "quorum"
            #  "x-dead-letter-routing-key" : "dl", # if not specified, queue's routing-key is used
            }
        )
        print(' [*] Waiting for messages on [' +  queue_name +'], to exit press CTRL+C')
        
        # prefetch_count=1 表示一次不能給一個工作者多個消息
        # 也就是說直到worker確認處理消息前不給他第二個message,會發給比較不忙的worker
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(queue_name,
                        on_message_callback=callback)

        channel.start_consuming()
    
    # Don't recover if connection was closed by broker
    except pika.exceptions.ConnectionClosedByBroker:
        print(pika.exceptions.ConnectionClosedByBroker)
        break
    # Don't recover on channel errors
    except pika.exceptions.AMQPChannelError:
        print(pika.exceptions.AMQPChannelError)
        break
    # Recover on all other connection errors
    except pika.exceptions.AMQPConnectionError:
        print(pika.exceptions.AMQPConnectionError)
        continue
