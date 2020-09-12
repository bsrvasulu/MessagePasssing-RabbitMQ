# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:23:36 2020

@author: Sreeni
"""
import pika
#import sys
import traceback
import json
import sys
import time

class ClientSubscribe(object):
    def __init__(self, queue_name):
        self.parameters = None
        self.connection = None
        self.channel = None
        self.queue_name = queue_name
        
        
    def createSubscriber(self):
        print('Create publisher start...')
        try: 
            self.parameters = pika.URLParameters('amqp://user:password@ip+address:5672/%2F')    
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()        
            self.channel.queue_declare(queue=self.queue_name, durable=True)         
            print(' [*] Waiting for messages. To exit press CTRL+C')
        except Exception as e:
            print('Fail to start process. Exception Message: '+ str(e))
            traceback.print_stack()
            print(repr(traceback.extract_stack()))
            print(repr(traceback.format_stack()))
            
    def callback(self, ch, method, properties, body):
        try:
            print(" [x] Received %r" % body)
            jsonData = json.loads(body)
            print(" [x] json data %r" % jsonData)
            time.sleep(1)#body.count(b'.'))
            print(" [x] Done")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
		    ch.basic_nack(delivery_tag=method.delivery_tag, multiple=False, requeue=True)
            print('Fail to start process. Exception Message: '+ str(e))
            traceback.print_stack()
            print(repr(traceback.extract_stack()))
            print(repr(traceback.format_stack()))
            
    def startReceiveMessage(self):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback)            
            self.channel.start_consuming()
        except Exception as e:
            print('Fail to start process. Exception Message: '+ str(e))
            traceback.print_stack()
            print(repr(traceback.extract_stack()))
            print(repr(traceback.format_stack()))
            
if __name__ == "__main__":
    queue_name = 'test.move'
    if (len(sys.argv) >= 2):
        queue_name = sys.argv[1]

    client_sub = ClientSubscribe(queue_name)
    client_sub.createSubscriber()
    client_sub.startReceiveMessage()
        