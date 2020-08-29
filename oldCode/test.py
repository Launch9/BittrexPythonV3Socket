from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.hub_connection_builder import logging
import signalrcore as sc
server_url = "wss://socket-v3.bittrex.com/signalr"

hub_connection = HubConnectionBuilder()\
    .with_url(server_url)\
    .configure_logging(logging.DEBUG)\
    .with_automatic_reconnect({
        "type": "raw",
        "keep_alive_interval": 10,
        "reconnect_interval": 5,
        "max_attempts": 5
    }).build()

hub_connection.on_open(lambda: print("connection opened and handshake received ready to send messages"))
hub_connection.on_close(lambda: print("connection closed"))
#hub_connection.on("ReceiveMessage", print)

hub_connection.start()

#hub_connection.send("Subscribe", ["tickers"])