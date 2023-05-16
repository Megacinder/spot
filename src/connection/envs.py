from os import path, getcwd, listdir
from pathlib import Path

ENV_FILE = '.env'


def search_for_the_file(**kwargs) -> Path:
    cur_dir = getcwd()
    file1 = kwargs.get('env_file', ENV_FILE)
    path1 = kwargs.get('env_path')

    if any(kwargs):
        if file1:
            cur_dir = path.dirname(cur_dir)
        elif path1:
            cur_dir = path.dirname(path1)
            file1 = ENV_FILE

    while True:
        if file1 in listdir(cur_dir):
            break
        elif cur_dir == path.dirname(cur_dir) or cur_dir == '/' or not cur_dir:
            raise FileNotFoundError(f"wrong filename: {file1}, can't find it")
        else:
            cur_dir = path.dirname(cur_dir)

    return Path(cur_dir, file1)


def envs(**kwargs) -> dict:
    file_path = search_for_the_file(**kwargs)
    print(file_path)
    env_dict = dict()
    with open(file_path, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            if '=' not in line:
                continue
            k, v = line.replace('\n', '').split('=')
            env_dict[k] = v
    return env_dict


if __name__ == "__main__":
    # envs = envs()
    for k, v in envs().items():
        print(f'{k}={v}')

    # from zipfile import ZipFile
    #     ar.extract()
    #     ar.printdir()
