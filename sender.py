#!/usr/bin/env python
import time
import pika

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

def send_msg(exange_name, msg):
    # create connection with login
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PWD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_VIRTUALHOST, credentials))
    
    # Create connection by default
    #connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    
    channel = connection.channel()
    channel.basic_publish(exchange= exange_name,
                      routing_key='',
                      body= msg)
    print(" [x] Sent " + msg)


exange_name = 'giant.direct'
msg = 'Hello Word!!!' +  time.ctime()
send_msg(exange_name, msg)





