import pycurl_requests as requests
from typing import Dict
from wetllib.adapters import ClickHouseIO
from time import sleep
from random import randint


BASE_URL_F = "https://api.opendota.com/api/players/{id}/"
players_set_query = '''
SELECT DISTINCT account_id
FROM dota2.players
'''

running_list_query = '''
SELECT DISTINCT account_id
FROM dota2.matches
WHERE cluster BETWEEN 181 AND 189 AND account_id != 4294967295
'''


headers = {
    'User-Agent': f'Random UA for parsing: {randint(10, 1000)}'
}


class TooManyRequests(Exception):
    pass


def get_player_info(account_id: int) -> Dict:
    res = requests.get(BASE_URL_F.format(id=account_id), headers=headers)
    print(f'{account_id}: response {res.status_code}')
    if res.status_code == 429:
        raise TooManyRequests()

    res = res.json()
    if len(res['mmr_estimate']) > 0:
        mmr_estimate = res['mmr_estimate']['estimate']
    else:
        mmr_estimate = None

    return {
        'account_id': account_id,
        'name': res['profile']['personaname'],
        'steamid': int(res['profile']['steamid']),
        'country': str(res['profile']['loccountrycode']),
        'solo_competitive_rank': res['solo_competitive_rank'],
        'competitive_rank': res['competitive_rank'],
        'rank_tier': res['rank_tier'],
        'leaderboard_rank': res['leaderboard_rank'],
        'mmr_estimate': mmr_estimate
    }


known_players_set = set(ClickHouseIO.get_data(players_set_query, 'localhost')['account_id'])
players_list = ClickHouseIO.get_data(running_list_query, 'localhost')['account_id']

players_info = []
for i, account_id in enumerate(players_list):
    if account_id in known_players_set:
        continue

    try:
        info = get_player_info(account_id)
    except TooManyRequests:
        break
    except (TypeError, KeyError):
        print(f'Fail for {account_id}, passed')
        continue
    else:
        players_info.append(info)

    if len(players_info) > 500:
        break

    sleep(0.3)


if len(players_info) > 0:
    ClickHouseIO.put_dict(players_info, 'dota2.players', 'localhost', [key for key in players_info[0]])
