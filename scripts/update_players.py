import subprocess
from time import sleep


command = "python3 players.py"


while True:
    subprocess.run(command.split(), check=True)
    sleep(7)
