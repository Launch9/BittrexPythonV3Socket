from signalr_aio import Connection
from base64 import b64decode, b64encode
from zlib import decompress, MAX_WBITS, compress
import json
import os
import time
import math
import sys
import time
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
FOLDER_NAME = "BittrexData"
FOLDER_2 = "orderBook"
FOLDER_3 = "trade"
orderBookCache = {}
metaCache = {}
MARKETS = ["BTC-USDT", "BAT-BTC"]
InvokableSubscriptions = ["orderbook_BTC-USDT_500","orderbook_BAT-BTC_500"]
dataPerHour = 0.0
MAX_CACHE = 100
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

def saveJsontoFile(jsonMessage, fileLocation, shouldCompress=True):
    fileSize = 0.0
    if(shouldCompress == True):
        stringifiedJson = str(jsonMessage).encode('utf-8')
        compressedJson = compress(stringifiedJson)
        reencoded = b64encode(compressedJson)
        fileSize = sys.getsizeof(reencoded)
        with open(fileLocation, "w") as f:
            f.write(str(reencoded))
    else:
        fileSize = sys.getsizeof(jsonMessage)
        with open(fileLocation, 'w') as f:
            json.dump(jsonMessage, f)
        print("Saved json file: " + str(fileLocation))
    return fileSize

def createFolderStructure(location):
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
    curCache["cache"].append(decoded_msg)
    if(len(curCache["cache"]) > MAX_CACHE):
        curCacheCopy = curCache["cache"].copy()
        curCache["cache"].clear()
        directory = os.path.join(FOLDER_NAME,FOLDER_2)
        mktDirectory = os.path.join(directory, mktSym)
        seq1 = curCacheCopy[0]["sequence"]
        seq2 = curCacheCopy[len(curCacheCopy) - 1]["sequence"]
        fileName = str(seq1) + "-" + str(seq2) + "-" + mktSym + ".obtx"
        curCacheCopy = compressJsonOrderBook(curCacheCopy)
        sizeOfFile = saveJsontoFile({"time":time.time(), "mktSym": mktSym, "depth": depth, "cache":curCacheCopy}, os.path.join(mktDirectory,fileName),True)
        curCache["meta"]["sizeof"].append(sizeOfFile)
        curCache["meta"]["lastTime"].append(time.time())
        timeDifference = str(curCache["meta"]["lastTime"][len(curCache["meta"]["lastTime"]) - 1] - curCache["meta"]["lastTime"][len(curCache["meta"]["lastTime"]) - 2])
        avgTimeDiff = 0
        avgSize = 0
        for i in range(len(curCache["meta"]["lastTime"]) - 1):
            avgTimeDiff += curCache["meta"]["lastTime"][i + 1] - curCache["meta"]["lastTime"][i]
        avgTimeDiff = avgTimeDiff / (len(curCache["meta"]["lastTime"]) - 1)
        for i in curCache["meta"]["sizeof"]:
            avgSize += i / 1000
        avgSize = avgSize / len(curCache["meta"]["sizeof"])
        dataRate = avgSize / avgTimeDiff
        dataRatePerHour = dataRate * 3600 #Hours
        dataRatePerDay = dataRatePerHour * 24 #Days
        curCache["meta"]["kbPerHour"] = dataRatePerHour
        print(bcolors.OKGREEN + "Finished " + mktSym + " cache! Time: " + str(timeDifference) + " seconds Size: " + str(sizeOfFile) + " bytes" + " HourlyRate: " + str(int(dataRatePerHour)) + "kb/hour" + " DailyRate: " + str(int(dataRatePerDay)) + "kb/day" + bcolors.ENDC)
        
    printString = ""
    for i in orderBookCache:
        curMkt = orderBookCache[i]
        printString += str(len(curMkt["cache"])) + "/" + str(MAX_CACHE) + "-" + i + "-"
        printString += str(int(curMkt["meta"]["kbPerHour"])) + "kb/hr "
    print(printString)
    #print(str(len(curCache["cache"])) + "/" + str(MAX_CACHE) + " - " + mktSym)

def initializeCaches():
    for i in MARKETS:
        orderBookCache[i] = {"cache":[],"meta":{"lastTime":[time.time()],"sizeof":[], "kbPerHour":0.0}}
        


if __name__ == "__main__":
    
    initializeCaches()
    #Creating folders if needed
    createFolderStructure("./")
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
    hub.server.invoke("Subscribe",InvokableSubscriptions)
    # Start the client
    connection.start()