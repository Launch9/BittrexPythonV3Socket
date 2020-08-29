from signalr_aio import Connection
from base64 import b64decode
from zlib import decompress, MAX_WBITS
import json

def process_message(message):
    message += "=" * ((4 - len(message) % 4) % 4)
    deflated_msg = decompress(b64decode(message), -15)
    return json.loads(deflated_msg.decode())

def process_message2(message):
    message += "=" * ((4 - len(message) % 4) % 4)
    deflated_msg = decompress(b64decode(message), 15)
    return json.loads(deflated_msg.decode())

# Create debug message handler.
async def on_debug(*args, **msg):
    # In case of 'queryExchangeState'
    #if 'R' in msg and type(msg['R']) is not bool:
    #decoded_msg = process_message2(msg['C'])
    print("on_debug!")

# Create error handler
async def on_error(msg):
    print(msg)


# Create hub message handler
async def on_message(msg):
    decoded_msg = process_message(msg[0])
    print(decoded_msg)

if __name__ == "__main__":
    # Create connection
    # Users can optionally pass a session object to the client, e.g a cfscrape session to bypass cloudflare.
    connection = Connection('https://socket-v3.bittrex.com/signalr', session=None)

    # Register hub
    hub = connection.register_hub('c3')

    # Assign debug message handler. It streams unfiltered data, uncomment it to test.
    connection.received += on_debug

    # Assign error handler
    connection.error += on_error

    # Assign hub message handler
    #hub.client.on('heartbeat', on_message)
    #hub.client.on('ticker_BTC-USD', on_message)
    #hub.client.on('orderbook', on_message)
    hub.client.on('orderBook', on_message)
    # Send a message
    hub.server.invoke("Subscribe",["orderbook_ETH-BTC_25"])
    #hub.server.invoke('SubscribeToSummaryDeltas')
    #hub.server.invoke('queryExchangeState', 'BTC-NEO')

    # Start the client
    connection.start()