from base64 import b64decode
from zlib import decompress, MAX_WBITS
"d-3350A2B9-B,0|Vho,0|Vhp,1"
import json
def process_message(message):
    #message += "=" * ((4 - len(message) % 4) % 4)
    deflated_msg = decompress(b64decode(message), 15+16)
    return json.loads(deflated_msg.decode())

process_message('J7bwYHbm3pjmdwRnxjKsZwKkvkCOBizcS9u1QQyDH1eqIzbLcwKQfIDaHvhXBzvUWtauzeRtniXrOj7Y9RJ+EbCH8FV7nFG1q0Z1W4EoSiiGqeqiMCCMVeUWPOsbF8IPpIJzbtii//tFTyV2FkNYbj/GBWk=')