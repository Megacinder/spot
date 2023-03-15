from os import environ, path, getcwd, listdir
from pathlib import Path
from typing import Union, Tuple


def search_for_the_file(*args: Union[Path, str, Union[Path, str]]) -> Path:
    cur_dir = getcwd()
    filename = '.env'

    if any(args):
        if len(args) == 1:
            if path.isfile(args[0]):
                file_path = args[0]
                cur_dir = path.dirname(file_path)
                filename = path.basename(file_path)
            elif path.isdir(args[0]):
                cur_dir = path.dirname(args[0])
                filename = '.env'
            else:
                cur_dir = getcwd()
                filename = args[0]
        elif len(args) == 2:
            cur_dir, filename = args
            print('cur, f: ', cur_dir, filename)

        else:
            raise ValueError(f"wrong filename or (path, filename) parameters: {args}")

    while True:
        if filename in listdir(cur_dir):
            break
        elif cur_dir == path.dirname(cur_dir) or cur_dir == '/' or not cur_dir:
            raise FileNotFoundError(f"wrong filename: {filename}, can't find it")
        else:
            cur_dir = path.dirname(cur_dir)

    return Path(cur_dir, filename)


def envs(*args: Union[Path, str, Tuple[Path, str]]) -> dict:
    file_path = search_for_the_file(*args)
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
    print(envs())
