import json
from base64 import b64encode, b64decode
from zlib import decompress, MAX_WBITS, compress
import json
data = {}
with open("./580142-580242.json") as f:
    data = json.load(f)

def saveJsontoFile(jsonMessage, fileLocation, shouldCompress=False):
    if(shouldCompress == True):
        stringifiedJson = str(jsonMessage).encode('utf-8')
        compressedJson = compress(stringifiedJson)
        reencoded = b64encode(compressedJson)
        with open(fileLocation, "w") as f:
            f.write(str(reencoded))

def stripBytes(code):
    return code[2:][:-1]

def readJsonfromFile(fileLocation):
    data = ""
    with open(fileLocation, "r") as f:
        data = f.read()
    data = stripBytes(data)
    decodedBytes = b64decode(data)
    decompressedBytes = decompress(decodedBytes)
    decompressedText = eval(stripBytes(str(decompressedBytes)))
    jsonObject = dict(decompressedText)
    return jsonObject

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

saveJsontoFile(data, "./testingFile.btx", True)
result = readJsonfromFile("./BittrexData/orderBook/BTC-USDT/783746-783846-BTC-USDT.obtx")
newJson = {"time":result["time"], "mktSym": result["mktSym"], "depth": result["depth"], "cache":compressJsonOrderBook(result["cache"])}
saveJsontoFile(newJson,"./testingFile.obtx",True)
print(result)
