# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:23:36 2020

@author: Sreeni
"""
import pika
import sys
import traceback
import json
import random

class ClientPublish(object):
    def __init__(self):
        self.parameters = None
        self.connection = None
        self.channel = None
    
    def ack_nack_callback(self, method_frame):
        print('msgPublished')
        
    def publishMessage(self, exhange, routing_key, msg):
        print('start publish msg: ' + str(msg))
        try:
            message = json.dumps(msg)
            self.parameters = pika.URLParameters('amqp://username:passward@ip_address:5672/%2F')    
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel() 
            self.channel.confirm_delivery()             
            self.channel.basic_publish(
                mandatory=True,
                exchange=exhange,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ))
            
            #print(was_delivered)
            print(" published message " + str( message))
            self.connection.close()
        except pika.exceptions.AMQPError as ure:
             print('Fail to start process. Exception Message: '+ str(ure))         
        except Exception as e:
            print('Fail to start process. Exception Message: '+ str(e))
            traceback.print_stack()
            print(repr(traceback.extract_stack()))
            print(repr(traceback.format_stack()))
            
if __name__ == "__main__":
    routing_key = 'text.move'
    exchange_name = 'exchange-1'
    number_msg = 1
    if (len(sys.argv) >= 2):
        number_msg = int(sys.argv[1])
    if (len(sys.argv) >= 3):
        exchange_name = sys.argv[2]    
    if (len(sys.argv) >= 4):
        routing_key = sys.argv[3]    

        
    client_publish = ClientPublish()
    for i in range(number_msg):
        rand_num = random.randint(1, 1000)
        data = {}
        data['msg_id'] = str(i) + str('_') + str(rand_num)
        data['source'] = '/nas02/image/disk_image_' + str(rand_num) + 'tif'
        data['destination'] = '/x509/image/disk_image_' + str(rand_num) + 'tif'
        client_publish.publishMessage(exchange_name, routing_key, data)

        