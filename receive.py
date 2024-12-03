#!/usr/bin/env python
import time
import random
import pika

# Default test host in GTM is 172.16.1.57
# Web UI http://172.16.1.57:15672/#/
RABBITMQ_URL = 'your-broker-url.mq.region.amazonaws.com'
# default port is 5672
RABBITMQ_PORT = 5671

# gient_dev_sender is only send to Exange giant.direct for permission sample
RABBITMQ_USER = 'your_username'
RABBITMQ_PWD  = 'your_password'

# default virtual host 
RABBITMQ_VIRTUALHOST = '/'

# 创建连接字符串
AMQP_URL = f"amqps://{RABBITMQ_USER}:{RABBITMQ_PWD}@{RABBITMQ_URL}:{RABBITMQ_PORT}"

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
    # [x] Received 'Hello Word!!!Sun Jul 28 16:24:58 2024'
    
    ## Do something
    print(" >>>>> Do something for " + body.decode())
    time.sleep(2)

    if random.random() < 0.5:
        #如果正確處理，回傳delivery_tag，queue 會被移除
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(" [O] Done \n")
    else:
        #如果處理異常，回傳 reject，資料會被丟到 dead queue
        ch.basic_reject(delivery_tag = method.delivery_tag, requeue=False)
        print(" [X] Rejected, send to [giant.dead] \n")



while True:
    try:
        # create connection
        parameters = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(parameters) 
        channel = connection.channel()
        queue_name = 'giant.direct.queue-1'
        
        # Check queue
        channel.queue_declare(queue_name, durable = True,
            arguments={
            #    "x-dead-letter-exchange" : "giant.dead",
            #    "x-queue-type" : "quorum"
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
        break
    # Don't recover on channel errors
    except pika.exceptions.AMQPChannelError:
        break
    # Recover on all other connection errors
    except pika.exceptions.AMQPConnectionError:
        continue
