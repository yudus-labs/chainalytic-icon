from pathlib import Path
import importlib
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import plyvel

from chainalytic_icon.common import config, util


class BaseTransform(object):
    """
    Base class for different Transform implementations

    Properties:
        working_dir (str):
        transform_id (str):
        kernel (Kernel):
        transform_storage_dir (str):
        transform_cache_dir (str):
        transform_cache_db (plyvel.DB):

    Methods:
        execute(height: int, input_data: Dict) -> Dict

    """

    def __init__(self, working_dir: str, transform_id: str):
        super(BaseTransform, self).__init__()
        self.working_dir = working_dir
        self.transform_id = transform_id
        self.kernel = None

        self.config = config.get_config(working_dir)
        storage_dir = (
            Path(working_dir, self.config['storage_dir'])
            .as_posix()
            .format(network_name=self.config['network_name'])
        )

        self.transform_storage_dir = self.config['transform_storage_dir'].format(
            storage_dir=storage_dir, transform_id=transform_id
        )
        self.transform_cache_dir = self.config['transform_cache_dir'].format(
            storage_dir=storage_dir, transform_id=transform_id
        )

        Path(self.transform_cache_dir).parent.mkdir(parents=1, exist_ok=1)
        self.transform_cache_db = plyvel.DB(self.transform_cache_dir, create_if_missing=True)

        self.logger = util.get_child_logger('aggregator.transform')

    def set_kernel(self, kernel: 'Kernel'):
        self.kernel = kernel

    async def execute(self, height: int, input_data: Any) -> Dict:
        return {'height': height, 'block_data': {}, 'latest_state_data': {}}


def load_transforms(working_dir: str) -> Dict:
    """
    Return a dict of loaded `Transform` modules
    """

    valid_transforms = config.get_config(working_dir)['transforms']
    ret = {}
    cur_dir = Path(__file__).resolve().parent
    for p in cur_dir.joinpath('transform_registry').glob('[!^_]*.py'):
        if p.stem not in valid_transforms and valid_transforms:
            continue
        spec = importlib.util.spec_from_file_location(p.name, p.as_posix())
        module = importlib.util.module_from_spec(spec)
        ret[p.stem] = module
        spec.loader.exec_module(module)

    return ret
