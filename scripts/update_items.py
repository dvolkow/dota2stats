import requests
import json
from wetllib.adapters import ClickHouseIO


token = "F054D314584DB7A5C6A1F8496680B485"

items_url_f = "https://api.steampowered.com/IEconDOTA2_570/GetGameItems/v1/?key={key}"
res = json.loads(requests.get(items_url_f.format(key=token)).text)

items = res['result']['items']
if len(items) > 0:
    ClickHouseIO.put_dict(items, 'dota2.items', 'localhost', [key for key in items[0]])
    print('Successfully updated!')
