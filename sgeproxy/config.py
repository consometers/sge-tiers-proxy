import os
import json


class File:
    def __init__(self, json_file_path):
        with open(json_file_path, "r") as json_file:
            self.conf = json.load(json_file)
        self.path = json_file_path

    def __getitem__(self, key):
        return self.conf[key]

    def abspath(self, relpath):
        cwd = os.getcwd()
        try:
            os.chdir(os.path.dirname(self.path))
            path = os.path.expanduser(relpath)
            path = os.path.abspath(path)
            return path
        finally:
            os.chdir(cwd)
