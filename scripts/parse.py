import argparse
import datetime as dt
import json
import requests
from wetllib.adapters import ClickHouseIO
from copy import deepcopy


parser = argparse.ArgumentParser(description="Dota 2 matches parsing")
parser.add_argument("--id", dest="id", help="Match ID for start")
parser.add_argument("--config", dest="config", help="Configuration filename")
parser.add_argument("--seq-size", dest="seq_size", default=15, help="Sequence size")
parser.add_argument("--skill", dest="skill", help="Skill level")
parser.add_argument("--mode", dest="mode", default="actual", help="Parse mode (actual, history)")

args = parser.parse_args()


with open(args.config, 'r') as f:
    settings = json.load(f)


query = '''
SELECT DISTINCT match_id
FROM dota2.matches
'''

known_matches = set(ClickHouseIO.get_data(query, 'localhost')['match_id'])
local_known = set()


token = settings['token']
if args.skill:
    skill = args.skill
else:
    skill = settings['skill']

player_info = set(settings['player_info'])
match_info = set(settings['match_info'])
ints = set(settings['types']['int'])
strs = set(settings['types']['str'])

match_history_f = settings['match_history_url_f']
match_history_seq_f = settings['match_history_seq_url_f']
match_details_f = settings['match_details_url_f']

last_id = args.id
seq_size = args.seq_size

for i in range(seq_size):
    players = []

    if args.mode == 'actual':
        res = json.loads(requests.get(match_history_f.format(key=token, id=last_id, skill=skill)).text)
    elif args.mode == 'history':
        res = json.loads(requests.get(match_history_seq_f.format(key=token, num=last_id, skill=skill)).text)
    else:
        raise ValueError(f'Bad mode {args.mode}')

    print(f'[{skill}]: Load matches from {last_id} ({len(res["result"]["matches"])} received)')

    matches = 0
    for match in res['result']['matches']:
        if int(match['match_id']) in known_matches or match['match_id'] in local_known:
            print(f'[{skill}]: Match {match["match_id"]} already in database, passed')
            continue

        if match['lobby_type'] in [7, 0]:
            matches += 1
            item = {
                'lobby_type': match['lobby_type'],
                'match_id': match['match_id'],
                'start_time': dt.datetime.fromtimestamp(int(match['start_time'])),
                'skill': skill
            }

            match_details = json.loads(requests.get(match_details_f.format(key=token, id=match['match_id'])).text)
            if len(match_details) == 0:
                print(f'[{skill}]: Fail for match {match["match_id"]}')
                continue

            item.update({
                'cluster': match_details['result']['cluster'],
                'game_mode': match_details['result']['game_mode'],
                'radiant_score': match_details['result']['radiant_score'],
                'dire_score': match_details['result']['dire_score'],
            })

            for player in match_details['result']['players']:
                new_player = deepcopy(item)
                for name in player_info:
                    new_player.update({name: player[name]})

                for name in ints:
                    new_player[name] = int(new_player[name])

                for name in strs:
                    new_player[name] = str(new_player[name])

                players.append(new_player)
                local_known.add(match['match_id'])

        if args.mode == 'actual':
            last_id = match['match_id']
        elif args.mode == 'history':
            last_id = match['match_seq_num']
        else:
            raise ValueError(f'Bad mode {args.mode}')

    if len(players) > 0:
        ClickHouseIO.put_dict(players, 'dota2.matches', 'localhost', [key for key in players[0]])
        print(f'[{skill}]: Save {matches} matches.')

