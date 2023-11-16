from .services import *


def renderMap():
    map = initMap()
    network = getData()

    map_realtime(network=network, map=map)
    # map_routes(map=map)


    return mapHTML(), mapTag(map=map)

