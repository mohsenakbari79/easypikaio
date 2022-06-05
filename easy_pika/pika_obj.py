
# from email import header
import pika
from pika.adapters.asyncio_connection import AsyncioConnection
import asyncio
import logging

logging.basicConfig(format='%(filename)s-%(process)d-%(levelname)s ->:%(message)s-')


class PkConnection():
    
    def __init__(self,url,exchange_name,exchange_type):
        self._url=url
        self.exchange_name=exchange_name
        self.exchange_type=exchange_type
        self._connection=None
        self.isExchangeDecleard=False
        self.callback=None

            
    async def connect(self,future):
        """Creates return new Singleton database connection"""
        logging.info(f'Connecting to {self._url}' )
        self.future=future
        self._connection=  AsyncioConnection(
            parameters=pika.URLParameters(self._url),
          
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)
        

    def on_connection_open(self,_unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        logging.info('Connection opened')
        self.open_channel()

    
        
    def on_connection_open_error(self,_unused_connection,err):
        # TODO: recoonect
        logging.error("connection oppen error")
        self.future.set_exception(f"connection oppen error: {err}")
        
    def on_connection_closed(self,_unused_connection, reason):
        # TODO: recoonect
        logging.error(f"connection oppen error :{reason}")


    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        logging.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)
        # return cha


    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        logging.info('Channel opened')
        self._channel = channel 
        self.future.set_result("self._channel")


    def create_consume(self):
        self.add_on_channel_close_callback()
        self.setup_exchange()


    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logging.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed) 

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        logging.info('Channel %i was closed: %s', channel, reason)
        # self.close_connection() 
        # TODO : handel close connection
    

    def setup_exchange(self):
        self._channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type,callback=self.on_exchange_declared)
        
    def on_exchange_declared(self, _unused_frame):
        self.isExchangeDecleard=True
        logging.info("exchange declared")



    async def bind_queue(self,future,queue_name,routing_keyes,on_message=None):
        self.qu_future=future
        self.on_message=on_message
        self.queue_name=queue_name
        self._channel.queue_declare(queue_name)
        if 0<len(routing_keyes):
            for rout in routing_keyes:
                self.on_queue_declared(rout)
        self.on_bind_queue_ok()
        return self._channel



    def on_queue_declared(self,routing):

        # for routing_key in routing_keyes:
        self._channel.queue_bind(
            self.queue_name, 
            self.exchange_name, 
            routing_key=routing,
            )
        
        

    def on_bind_queue_ok(self):
        self._channel.basic_qos(prefetch_count=1,callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self,unused):
        self._channel.add_on_cancel_callback(self.on_consumer_cancel)
        self._channel.basic_consume(self.queue_name,self.on_message,callback=self.on_basic_consume_ok)
    def on_consumer_cancel(self,onuse):
        logging.info(onuse,"consumer cancel")
    def on_basic_consume_ok(self,unused):
        self.qu_future.set_result(self._channel)
    async def publish(self,routing_key,data,headers):
        self._channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=data,properties=pika.BasicProperties(
                          headers=headers# Add a key/value header
                      ),
        )

class PikaMassenger():

    def __init__(self,username,password,host,port,exchange_name,exchange_type='direct') -> None:
        self.connection=PkConnection(f"amqp://{username}:{password}@{host}:{port}/%2F",exchange_name,exchange_type)
    async def connect(self):
        loop = asyncio.get_running_loop()
        # Create a new Future object.
        fut = loop.create_future()
        loop.create_task(self.connection.connect(fut))
        await fut
        logging.info("connected succusfully")



    def add_queue(self,routing_keyes):
        self.connection.on_queue_declared(routing_keyes)

    async def consume(self,queue_name,routing_keyes,callback):
        self.queue_name=queue_name
        self.connection.create_consume()
        loop = asyncio.get_running_loop()
        # Create a new Future object.
        fut = loop.create_future()
        loop.create_task(self.connection.bind_queue(fut,queue_name,routing_keyes,callback))
        await fut

    
    def defualt_callback(self,ch, method, properties, body):
        logging.info("test callback : ",body)
    async def create_consume(self, queue_name, routing_keys ,callback):
        await self.connect()
        await self.consume(queue_name,routing_keys,callback)
    def run(self, queue_name, routing_keys ,callback):
        loop = asyncio.new_event_loop()
        foo = loop.run_until_complete(self.create_consume(queue_name, routing_keys,callback))
        self.connection._connection.ioloop.run_forever()
    async def send_message(self,routing_key,data):
        await self.connect()
        await self.connection.publish(routing_key,data)





