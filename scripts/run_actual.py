import subprocess


_command = "python3 parse.py --config settings.json --skill {skill}"


iteration = 0
while True:
    print(f'Start iteration # {iteration}:')
    for i in [1, 2, 3]:
        command = _command.format(skill=i).split()
        subprocess.run(command, check=True)
    iteration += 1
