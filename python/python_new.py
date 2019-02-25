import requests
import json
import pprint



def ConfigureAwsObject(symbolName):
    try:
        r = requests.post(globalHost, data='{"type":"ConfigureAws#","symbol":"'+symbolName+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def RemoveSymbol(symbolName):
    try:
        r = requests.post(globalHost, data='{"type":"RemoveSymbol#","symbol":"'+symbolName+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def ListSymbols():
    try:
        r = requests.post(globalHost, data='{"type":"ListSymbols#"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

#
# Ping the system
def Ping():
    try:
        r = requests.post(globalHost, data='{"type":"Ping#"}')
        data = json.loads(r.text)
        return r.status_code, data['timestamp']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

    
def Refresh():
    try:
        r = requests.post(globalHost, data='{"type":"Refresh#"}')
        r = requests.post(globalHost, data='{"type":"Refresh#"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['updated']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def GetPrice(camp,creat):
    try:
        r = requests.post(globalHost, data='{"type":"GetPrice#","campaign":"' + camp + '","creative":"'+creat+'"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['price']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def SetPrice(*arg):
    try:
        if len(arg)==2:
            campaign=arg[0];
            creative=arg[1];
            r = requests.post(globalHost, data='{"type":"SetPrice#","campaign":"' + campaign + '","creative":"'+creative+'"}')
        if len(arg)==3:
            campaign=arg[0];
            creative=arg[1];
            deal=arg[2];
            r = requests.post(globalHost, data='{"type":"SetPrice#","campaign":"' + campaign + '","creative":"'+creative+'", "deal":"'+deal+'"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['price']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None


def SetWeights(camp,weights):
    try:
        r = requests.post(globalHost, data='{"type":"SetWeights#","campaign":"' + camp + '","weights":"'+weights+'"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['asyncid']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def GetWeights(camp):
    try:
        r = requests.post(globalHost, data='{"type":"GetWeights#","campaign":"' + camp + '"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['asyncid']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

    
def GetBudget(*arg):
    try:
        if len(arg)==1:
            r = requests.post(globalHost, data='{"type":"GetBudget#","campaign":"' + arg[0] + '"}')
        if len(arg)==2:
            r = requests.post(globalHost, data='{"type":"GetBudget#","campaign":"' + arg[0] + '","creative":"' + arg[1] + '"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def SetBudget(camp,total,daily,hourly):
    try:
        tb = "%f" % total
        db = "%f" % daily
        hb = "%f" % hourly
        r = requests.post(globalHost, data='{"type":"SetBudget#","campaign":"' + camp  +
            '","total":' + tb + ',"daily":' + db + ',"hourly":' + hb + '}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def GetValues(*arg):
    try:
        if len(arg)==1:
            r = requests.post(globalHost, data='{"type":"GetValues#","campaign":"' + arg[0] + '"}')
        if len(arg)==2:
            r = requests.post(globalHost, data='{"type":"GetBudget#","campaign":"' + arg[0] + '","creative":"' + arg[1] + '"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def GetStatus(*arg):
    try:
        r = requests.post(globalHost, data='{"type":"GetStatus#"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def GetStatusA(*arg):
    try:
        r = requests.post(globalHost, data='{"type":"GetStatus#","async":true}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def ListCreatives(camp):
    try:
        r = requests.post(globalHost, data='{"type":"GetCampaign#","campaign":"'+camp+'"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            camp = data['node']
            creatives = camp['creatives']
            names = []
            for creat in creatives:
                names.append(creat['impid'])

            return r.status_code, data['timestamp'], data['error'], names
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def GetCreative(camp, crid):
    try:
        r = requests.post(globalHost, data='{"type":"GetCampaign#","campaign":"'+camp+'"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            camp = data['node']
            creatives = camp['creatives']
            for creat in creatives:
                if creat['impid'] == crid:
                    return r.status_code, data['timestamp'], data['error'], creat
            return r.status_code, data['timestamp'], data['error'], None
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def GetCampaign(camp):
    try:
        r = requests.post(globalHost, data='{"type":"GetCampaign#","campaign":"'+camp+'"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['node']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def DeleteCampaign(camp):
    try:
        r = requests.post(globalHost, data='{"type":"Delete#","campaign":"'+camp+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None


def GetReason(*arg):
    try:
        if len(arg)==0:
            r = requests.post(globalHost, data='{"type":"GetReason#"}')
        else:
            r = requests.post(globalHost, data='{"type":"GetReason#","campaign":"'+arg[0]+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def GetReasonA(*arg):
    try:
        if len(arg)==0:
            r = requests.post(globalHost, data='{"async":true, "type":"GetReason#"}')
        else:
            r = requests.post(globalHost, data='{"async": true,"type":"GetReason#","campaign":"'+arg[0]+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def GetSpendRate(*arg):
    try:
        if len(arg)==0:
            r = requests.post(globalHost, data='{"type":"GetSpendRate#"}')
        else:
            if len(arg)==1:
                r = requests.post(globalHost, data='{"type":"GetSpendRate#","campaign":"'+arg[0]+'"}')
            else:
                r = requests.post(globalHost, data='{"type":"GetSpendRate#","campaign":"'+arg[0]+'","creative":"'+arg[1]+'", "type":"'+arg[2]+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def ListCampaigns():
    try:
        r = requests.post(globalHost, data='{"type":"ListCampaigns#"}')
        data = json.loads(r.text)
        return r.status_code, data['timestamp'], data['campaigns']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def StartBidder(bidder):
    try:
        r = requests.post(globalHost, data='{"type":"StartBidder#","biddder":"'+bidder+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def StopBidder(bidder):
    try:
        r = requests.post(globalHost, data='{"type":"StopBidder#","biddder":"'+bidder+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def Update(camp):
    try:
        r = requests.post(globalHost, data='{"type":"Update#","campaign":"'+camp+'"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def RefreshA():
    try:
        r = requests.post(globalHost, data='{"type":"Refresh#","async":true}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def Future(id):
    try:
        r = requests.post(globalHost, data='{"type":"Future#","asyncid":"' + id + '"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['message']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def Dump():
    try:
        r = requests.post(globalHost, data='{"type":"Dump#"}')
        print (r.status_code, r.reason)
        print (r.text)
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None
    
def GetBiddersStatus():
    try:
        r = requests.post(globalHost, data='{"type":"GetBiddersStatus#"}')
        data = json.loads(r.text)
        if data['error']:
            return r.status_code, data['timestamp'], data['error'], data['message']
        else:
            return r.status_code, data['timestamp'], data['error'], data['entries']
    except requests.exceptions.RequestException as e:
        print('Connection error')
        return 503, None

def SetHost(host):
    global globalHost
    globalHost = "http://" + host + ":8100/api"
    return 200, globalHost

def SetHostPort(hostPort):
    global globalHost
    globalHost = "http://" + hostPort + "/api"
    return 200, globalHost

def PP(x):
    pp.pprint(x)
    
globalHost = "http://localhost:8100/api"
pp = pprint.PrettyPrinter(indent=4)

