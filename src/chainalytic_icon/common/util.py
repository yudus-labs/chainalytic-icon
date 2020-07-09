import json
import logging
from pathlib import Path
import os
from chainalytic_icon.common import config


def pretty(d: dict) -> str:
    return json.dumps(d, indent=2, sort_keys=1)


def create_logger(working_dir: str, logger_name: str, level: int = None):
    cfg = config.get_config(working_dir)

    if not level:
        level = cfg['log_level']
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    log_path = Path(cfg['log_dir'].format(network_name=cfg['network_name']), f'{logger_name}.log')
    log_path.parent.mkdir(parents=1, exist_ok=1)

    fh = logging.FileHandler(log_path.as_posix(), mode='w')
    fh.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s | %(name)s - %(levelname)s | %(message)s', '%m-%d %H:%M:%S'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def get_child_logger(logger_name: str):
    return logging.getLogger(logger_name)
