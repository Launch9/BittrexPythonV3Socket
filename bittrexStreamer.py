from signalr_aio import Connection
from base64 import b64decode, b64encode
from zlib import decompress, MAX_WBITS, compress
import json
import os
import time
import math
FOLDER_NAME = "BittrexData"
FOLDER_2 = "orderBook"
FOLDER_3 = "trade"
orderBookCache = {}
metaCache = {}
MARKETS = ["BTC-USDT"]
MAX_CACHE = 1000
def process_message(message):
    message += "=" * ((4 - len(message) % 4) % 4)
    deflated_msg = decompress(b64decode(message), -15)
    return json.loads(deflated_msg.decode())

# Create debug message handler.
async def on_debug(*args, **msg):
    print("on_debug!")

# Create error handler
async def on_error(msg):
    print(msg)

def compressJsonOrderBook(jsonMessage):
    output = []
    for b in jsonMessage:
        comBidDeltas = [[],[]]
        for i in b["bidDeltas"]:
            comBidDeltas[0].append(i["quantity"])
            comBidDeltas[1].append(i["rate"])
        comAskDeltas = [[],[]]
        for i in b["askDeltas"]:
            comAskDeltas[0].append(i["quantity"])
            comAskDeltas[1].append(i["rate"])
        output.append({"s":b["sequence"],"b":comBidDeltas,"a":comAskDeltas,"t":b["time"]})
    return output

def saveJsontoFile(jsonMessage, fileLocation, shouldCompress=False):
    if(shouldCompress == True):
        stringifiedJson = str(jsonMessage).encode('utf-8')
        compressedJson = compress(stringifiedJson)
        reencoded = b64encode(compressedJson)
        with open(fileLocation, "w") as f:
            f.write(str(reencoded))
    else:
        with open(fileLocation, 'w') as f:
            json.dump(jsonMessage, f)
        print("Saved json file: " + str(fileLocation))

def makeSureFolders(location):
    homeFolder = os.path.join(location,FOLDER_NAME)
    if(not os.path.exists(homeFolder)):
        os.mkdir(homeFolder)
        orderBookPath = os.path.join(homeFolder, FOLDER_2)
        os.mkdir(orderBookPath)
        for i in MARKETS:
            os.mkdir(os.path.join(orderBookPath, i))
        tradeBookPath = os.path.join(homeFolder, FOLDER_3)
        os.mkdir(tradeBookPath)
        for i in MARKETS:
            os.mkdir(os.path.join(tradeBookPath, i))
        print("New Folders Created!")
    else:
        print("Folders already exist.")




# Create hub message handler
async def on_message(msg):
    decoded_msg = process_message(msg[0])
    decoded_msg["time"] = time.time()
    mktSym = decoded_msg["marketSymbol"]
    depth = decoded_msg["depth"]
    curCache = orderBookCache[mktSym]
    curCache.append(decoded_msg)
    if(len(curCache) > MAX_CACHE):
        curCacheCopy = curCache["cache"].copy()
        curCache["cache"].clear()
        curCache["meta"]["lastTime"] = time.time()
        directory = os.path.join(FOLDER_NAME,FOLDER_2)
        mktDirectory = os.path.join(directory, mktSym)
        seq1 = curCacheCopy[0]["sequence"]
        seq2 = curCacheCopy[len(curCacheCopy) - 1]["sequence"]
        fileName = str(seq1) + "-" + str(seq2) + "-" + mktSym + ".obtx"
        curCacheCopy = compressJsonOrderBook(curCacheCopy)
        saveJsontoFile({"time":time.time(), "mktSym": mktSym, "depth": depth, "cache":curCacheCopy}, os.path.join(mktDirectory,fileName),True)
        
    #saveJsontoFile(decoded_msg,)
    print(str(len(curCache)) + "/" + str(MAX_CACHE) + " - " + mktSym)

def initializeCaches():
    for i in MARKETS:
        orderBookCache[i] = {"cache":[],"meta":{"lastTime":0.0,"sizeof":0.0}}
        


if __name__ == "__main__":
    
    initializeCaches()
    #Creating folders if needed
    makeSureFolders("./")
    # Create connection
    connection = Connection('https://socket-v3.bittrex.com/signalr', session=None)
    # Register hub
    hub = connection.register_hub('c3')
    # Assign debug message handler. It streams unfiltered data, uncomment it to test.
    #connection.received += on_debug
    # Assign error handler
    connection.error += on_error
    # Assign hub message handler
    hub.client.on('orderBook', on_message)
    # Send a message
    hub.server.invoke("Subscribe",["orderbook_BTC-USDT_500"])
    # Start the client
    connection.start()