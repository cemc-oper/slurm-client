# coding: utf-8
import os
import subprocess

from .config import get_config


def get_user_name() -> str:
    if 'USER' in os.environ:
        return os.environ["USER"]
    else:
        command = "whoami"
        pipe = subprocess.Popen([command], stdout=subprocess.PIPE, shell=True)
        return pipe.communicate()[0].decode().rstrip()
