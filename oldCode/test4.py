import os
os.environ['GEVENT_SUPPORT'] = 'True'
import gevent.monkey
gevent.monkey.patch_all()
from requests import Session
from signalr import Connection
import gzip
import base64, zlib
with Session() as session:
    #create a connection
    connection = Connection("https://socket-v3.bittrex.com/signalr", session)

    #get chat hub
    chat = connection.register_hub('c3')

    #start a connection
    connection.start()
    #create new chat message handler
    def print_received_message(data):
        t = bytearray(base64.b64decode(data))
        f = gzip.open('foo1.gz', 'wb')
        f.write(t)
        f.close()
        decoded_data = zlib.decompress(t,15 + 16)
        print('received: ', decoded_data)

    def msg_received(*args, **kwargs):
        message = kwargs["C"]
        message = bytearray(message)
        f = gzip.open('foo2.gz', 'wb')
        f.write(message)
        f.close()
        decoded_data = zlib.decompress(message,15 + 16)
        # args[0] contains your stream
        print(decoded_data)
        print("Nothing")

    #create new chat topic handler
    def print_topic(topic, user):
        print('topic: ', topic, user)

    #create error handler
    def print_error(error):
        print('error: ', error)

    #connection.received += msg_received
    connection.error += print_error
    
    #receive new chat messages from the hub
    chat.client.on('marketsummaries', print_received_message)
    chat.client.on('ticker_BTC-USD', print_received_message)
    #change chat topic
    #chat.client.on('topicChanged', print_topic)

    #process errors
    #connection.error += print_error

    #start connection, optionally can be connection.start()
    #with connection:

    #post new message
    returned = chat.server.invoke('Subscribe', {"H":"c3","M":"Subscribe","A":[["heartbeat","ticker_BTC-USD"]],"I":1})

    """#change chat topic
    chat.server.invoke('setTopic', 'Welcome python!')
    #invoke server method that throws error
    chat.server.invoke('requestError')
    #post another message
    chat.server.invoke('send', 'Bye-bye!')"""
    #wait a second before exit
    connection.wait(10)