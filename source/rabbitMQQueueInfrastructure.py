# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:23:36 2020

@author: Sreeni
"""
import pika
import traceback

class rabbitMQQueueInfrastructure(object):
    def __init__(self):
        self.parameters = None
        self.connection = None
        self.channel = None        
 
    def createExchange(self, exchangeName, exchangeType):
        try:
            print('Create exchange start...')
            self.parameters = pika.URLParameters('amqp://user:password@ip+address:5672/%2F')
            self.connection = pika.BlockingConnection(self.parameters)       
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchangeName, exchange_type=exchangeType, durable=True)
            self.connection.close()
            print('Exchange created ...')
            
        except Exception as e:
            print('Fail to create exchange. Exception Message: '+ str(e))
            traceback.print_stack()
            print(repr(traceback.extract_stack()))
            print(repr(traceback.format_stack()))
            
    def createPublisher(self, exchange, queue_name, routing_key):
        print('Create publisher start...')
        try: 
            self.parameters = pika.URLParameters('amqp://user:password@ip+address:5672/%2F')    
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()        
            chanel_queue_result = self.channel.queue_declare(queue=queue_name, durable=True)      
            self.channel.queue_bind(exchange=exchange, queue=chanel_queue_result.method.queue, routing_key=routing_key)
            self.connection.close()
            print('Publisher created...')
        except Exception as e:
            print('Fail to create publisher. Exception Message: '+ str(e))
            traceback.print_stack()
            print(repr(traceback.extract_stack()))
            print(repr(traceback.format_stack()))
          
if __name__ == "__main__":
    queue_infrastructure = rabbitMQQueueInfrastructure()
    queue_infrastructure.createExchange('test', 'topic')
    queue_infrastructure.createPublisher('test', 'test_connect', 'test.connect')
    queue_infrastructure.createPublisher('test', 'test_copy', 'test.move')
    queue_infrastructure.createPublisher('test', 'test_all', 'test.*')

        