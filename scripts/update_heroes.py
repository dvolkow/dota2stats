import requests
import json
from wetllib.adapters import ClickHouseIO


token = "F054D314584DB7A5C6A1F8496680B485"

items_url_f = "https://api.steampowered.com/IEconDOTA2_570/GetHeroes/v1/?key={key}"
res = json.loads(requests.get(items_url_f.format(key=token)).text)
heroes = res['result']['heroes']

if len(heroes) > 0:
    ClickHouseIO.put_dict(heroes, 'dota2.heroes', 'localhost', [key for key in heroes[0]])
    print('Successfully updated!')
