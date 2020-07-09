import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chainalytic_icon.common import config, util

from . import transform


class Kernel(object):
    """

    Properties:
        working_dir (str):
        transforms (dict):
        storage (Storage):

    Methods:
        add_transform(transform: Transform)
        execute(height: int, input_data: Dict, transform_id: str)
    """

    def __init__(self, working_dir: str, storage: 'Storage'):
        super(Kernel, self).__init__()
        self.working_dir = working_dir
        self.transforms = {}
        self.storage = storage
        self.config = config.get_config(working_dir)
        self.logger = util.get_child_logger('aggregator.kernel')

        transform_registry = transform.load_transforms(working_dir)
        for transform_id in transform_registry:
            t = transform_registry[transform_id].Transform(working_dir, transform_id)
            self.add_transform(t)

    def add_transform(self, transform: 'Transform'):
        self.transforms[transform.transform_id] = transform
        transform.set_kernel(self)

    async def execute(self, height: int, input_data: Any, transform_id: str) -> Optional[bool]:
        """Execute transform and push output data to storage
        """
        output = r = None
        if transform_id in self.transforms:
            try:
                output = await self.transforms[transform_id].execute(height, input_data)
            except Exception as e:
                self.logger.error(f'ERROR while executing transform {transform_id}')
                self.logger.error(str(e))
                self.logger.error(traceback.format_exc())
        if not output:
            return 0

        if transform_id == 'stake_history':
            r1 = self.storage.put_block(
                api_params={
                    'height': output['height'],
                    'block_data': output['block_data'],
                    'transform_id': transform_id,
                },
            )
            r2 = self.storage.set_latest_unstake_state(
                api_params={
                    'unstake_state': output['latest_state_data']['latest_unstake_state'],
                    'transform_id': transform_id,
                },
            )
            r = r1 and r2

        elif transform_id == 'stake_top100':
            r = self.storage.set_latest_stake_top100(
                api_params={
                    'stake_top100': output['latest_state_data']['latest_stake_top100'],
                    'transform_id': transform_id,
                },
            )

        elif transform_id == 'recent_stake_wallets':
            r = self.storage.set_recent_stake_wallets(
                api_params={
                    'recent_stake_wallets': output['latest_state_data']['recent_stake_wallets'],
                    'transform_id': transform_id,
                },
            )

        elif transform_id == 'abstention_stake':
            r = self.storage.set_abstention_stake(
                api_params={
                    'abstention_stake': output['latest_state_data']['abstention_stake'],
                    'transform_id': transform_id,
                },
            )

        elif transform_id == 'funded_wallets':
            r = self.storage.update_funded_wallets(
                api_params={
                    'updated_wallets': output['latest_state_data']['updated_wallets'],
                    'transform_id': transform_id,
                },
            )

        elif transform_id == 'passive_stake_wallets':
            r = self.storage.update_passive_stake_wallets(
                api_params={
                    'updated_wallets': output['latest_state_data']['updated_wallets'],
                    'transform_id': transform_id,
                },
            )

        elif transform_id == 'contract_history':
            r = self.storage.update_contract_history(
                api_params={
                    'updated_contract_state': output['latest_state_data']['updated_contract_state'],
                    'transform_id': transform_id,
                },
            )

        # Update last block height of corresponding `transform_id`
        if r:
            self.storage.set_last_block_height(
                api_params={'height': output['height'], 'transform_id': transform_id,}
            )
