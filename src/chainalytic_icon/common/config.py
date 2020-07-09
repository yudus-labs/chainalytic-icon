import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ruamel.yaml import YAML

_WORKING_DIR = os.getcwd()
CHAINALYTIC_FOLDER = '.chainalytic_icon'
CFG_FOLDER = 'cfg'


def set_working_dir(wd: str):
    """
    Args:
        wd (str): new working directory
    """
    global _WORKING_DIR
    _WORKING_DIR = wd


def get_working_dir() -> str:
    return _WORKING_DIR


def check_user_config(working_dir: str = get_working_dir()) -> bool:
    user_config_dir = Path(working_dir, CHAINALYTIC_FOLDER, CFG_FOLDER)
    return user_config_dir.joinpath('config.yml').exists()


def init_user_config(working_dir: str = get_working_dir()) -> Dict[str, str]:
    default_config_dir = Path(__file__).resolve().parent.joinpath('default_cfg')
    user_config_dir = Path(working_dir, CHAINALYTIC_FOLDER, CFG_FOLDER)

    assert default_config_dir.exists(), 'Default config not found'

    def_config_path = Path(default_config_dir, 'config.yml')
    user_config_path = Path(user_config_dir, 'config.yml')
    if def_config_path.exists() and not user_config_path.exists():
        user_config_path.parent.mkdir(parents=1, exist_ok=1)
        shutil.copyfile(
            def_config_path.as_posix(), user_config_path.as_posix(),
        )

    return user_config_path


def clean_user_config(working_dir: str = get_working_dir()):
    user_config_dir = Path(working_dir, CHAINALYTIC_FOLDER, CFG_FOLDER)
    shutil.rmtree(user_config_dir.as_posix(), ignore_errors=1)


def get_config(working_dir: str = get_working_dir()) -> Optional[Dict]:
    user_config_dir = Path(working_dir, CHAINALYTIC_FOLDER, CFG_FOLDER)
    user_config_path = Path(user_config_dir, 'config.yml')
    data = None
    try:
        with open(user_config_path) as f:
            yaml = YAML(typ='safe')
            data = yaml.load(f.read())
        if 'log_level' in data:
            os.environ['LOG_LEVEL'] = str(data['log_level'])
    except Exception:
        pass
    return data
