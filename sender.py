#!/usr/bin/env python
import time
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

def send_msg(exange_name, msg):
    # create connection with login
    parameters = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(parameters)    
    channel = connection.channel()
    channel.basic_publish(exchange= exange_name,
                      routing_key='',
                      body= msg)
    print(" [x] Sent " + msg)


exange_name = 'giant.direct'
msg = 'Hello Word!!!' +  time.ctime()
send_msg(exange_name, msg)





