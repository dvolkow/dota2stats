import argparse
import datetime as dt
import json
import pycurl_requests as requests
from wetllib.adapters import ClickHouseIO
from wetllib.scheduler import _to_time, DATEF
from copy import deepcopy


parser = argparse.ArgumentParser(description="Dota 2 matches parsing")
parser.add_argument("--id", dest="id", help="Account ID")
parser.add_argument("--match-id", dest="match_id", help="Start match ID")
parser.add_argument("--config", dest="config", help="Configuration filename")
parser.add_argument("--depth-date", dest="depth_date", help="Depth from date")
parser.add_argument("--seq-size", dest="seq_size", default=15, help="Sequence size")

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
skill = 0

player_info = set(settings['player_info'])
match_info = set(settings['match_info'])
ints = set(settings['types']['int'])
strs = set(settings['types']['str'])

limit = _to_time(args.depth_date, DATEF)

match_history_by_accid_url_f = settings['match_history_by_accid_url_f']
match_details_f = settings['match_details_url_f']

last_id = args.match_id
seq_size = int(args.seq_size)

for i in range(seq_size):
    try:
        players = []
        res = requests.get(match_history_by_accid_url_f.format(key=token, id=args.id, match_id=last_id)).json()

        print(f'#{i} [{skill}]: Load matches from {last_id} ({len(res["result"]["matches"])} received)')

        matches = 0
        for match in res['result']['matches']:
            if int(match['match_id']) in known_matches or match['match_id'] in local_known:
                print(f'#{i} [{skill}]: Match {match["match_id"]} already in database, passed')
                continue

            if match['lobby_type'] in [7, 0]:
                matches += 1
                item = {
                    'lobby_type': match['lobby_type'],
                    'match_id': match['match_id'],
                    'match_seq_num': match['match_seq_num'],
                    'start_time': dt.datetime.fromtimestamp(int(match['start_time'])),
                    'skill': skill
                }

                if item['start_time'] < limit:
                    print(f'reach last time limit: {item["start_time"]}')
                    raise StopIteration()

                match_details = requests.get(match_details_f.format(key=token, id=match['match_id'])).json()
                if len(match_details) == 0:
                    print(f'#{i} [skill {skill}]: Fail for match {match["match_id"]}')
                    continue

                print(f'Add info for match {match["match_id"]}')
                item.update({
                    'cluster': match_details['result']['cluster'],
                    'game_mode': match_details['result']['game_mode'],
                    'radiant_win': match_details['result']['radiant_win'],
                    'duration': match_details['result']['duration'],
                    'radiant_score': match_details['result']['radiant_score'],
                    'dire_score': match_details['result']['dire_score'],
                })

                for player in match_details['result']['players']:
                    new_player = deepcopy(item)
                    for name in player_info:
                        try:
                            new_player.update({name: player[name]})
                        except KeyError:
                            new_player.update({name: 0})

                    for name in ints:
                        new_player[name] = int(new_player[name])

                    for name in strs:
                        new_player[name] = str(new_player[name])

                    players.append(new_player)

                local_known.add(match['match_id'])

            last_id = match['match_id']
    except StopIteration:
        if len(players) > 0:
            ClickHouseIO.put_dict(players, 'dota2.matches', 'localhost', [key for key in players[0]])
            print(f'#Last [{skill}]: Save {len(players) // 10} matches.')
        break

    if len(players) > 0:
        ClickHouseIO.put_dict(players, 'dota2.matches', 'localhost', [key for key in players[0]])
        print(f'#{i} [{skill}]: Save {matches} matches.')

