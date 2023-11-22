from .services import *

def renderMap():
    network = getData()
    map = map_realtime(network=network)
    
    return mapTag(map=map)

