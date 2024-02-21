import json

def getRouteData(jmsType):
    with open("routes.json") as f:
        routes_data = json.load(f)
        for rd in routes_data.get("routes"):
            if rd.get('JMSType')==jmsType:
                return rd
    return None

print(getRouteData("firstType"))