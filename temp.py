import os
from os.path import dirname

def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        if 'venv' in dirs:
            dirs.remove('venv')

        if '__pycache__' in dirs:
            dirs.remove('__pycache__')

        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))


if __name__ == "__main__":
    list_files(dirname(__file__))