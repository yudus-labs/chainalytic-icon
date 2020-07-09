from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import traceback
from chainalytic_icon.common import config
from chainalytic_icon.common import util
from chainalytic_icon.common.util import get_child_logger
from chainalytic_icon.provider.api_bundle import BaseApiBundle


class ApiBundle(object):
    """
    The interface to external consumers/applications
    """

    def __init__(self, working_dir: str):
        super(ApiBundle, self).__init__()
        self.working_dir = working_dir
        self.collator = None

        self.logger = get_child_logger('provider.api_bundle')

    def set_collator(self, collator: 'Collator'):
        self.collator = collator

    async def api_call(self, api_id: str, api_params: dict) -> Dict:
        ret = {'status': 0, 'result': None}
        func = getattr(self, api_id) if hasattr(self, api_id) else None

        try:
            if func:
                self.logger.debug(f'Found API: {api_id}, calling...')
                ret['result'] = await func(api_params)
                ret['status'] = 1
            else:
                self.logger.warning(f'API not found: {api_id}')
                ret['status'] = -1
                ret['result'] = f'API not found: {api_id}'
        except Exception as e:
            ret['status'] = 0
            ret['result'] = f'{str(e)}\n{traceback.format_exc()}'
            self.logger.error(f'ERROR when calling API: {api_id}')
            self.logger.error(f'{str(e)}\n{traceback.format_exc()}')

        return ret

    async def get_staking_info(self, api_params: dict) -> Optional[dict]:
        if 'height' in api_params:
            return await self.collator.get_block(api_params['height'], 'stake_history')

    async def last_block_height(self, api_params: dict) -> Optional[int]:
        if 'transform_id' in api_params:
            return await self.collator.last_block_height(api_params['transform_id'])

    async def get_staking_info_last_block(self, api_params: dict) -> Optional[Dict]:
        height = await self.collator.last_block_height('stake_history')
        if height:
            r = await self.collator.get_block(height, 'stake_history')
            if r:
                r['height'] = height
                return r

    async def latest_unstake_state(self, api_params: dict) -> Optional[int]:
        return await self.collator.latest_unstake_state('stake_history')

    async def latest_stake_top100(self, api_params: dict) -> Optional[dict]:
        return await self.collator.latest_stake_top100('stake_top100')

    async def recent_stake_wallets(self, api_params: dict) -> Optional[dict]:
        return await self.collator.recent_stake_wallets('recent_stake_wallets')

    async def abstention_stake(self, api_params: dict) -> Optional[dict]:
        return await self.collator.abstention_stake('abstention_stake')

    async def funded_wallets(self, api_params: dict) -> Optional[dict]:
        return await self.collator.funded_wallets(
            'funded_wallets', float(api_params['min_balance']) if 'min_balance' in api_params else 1
        )

    async def passive_stake_wallets(self, api_params: dict) -> Optional[dict]:
        return await self.collator.passive_stake_wallets(
            'passive_stake_wallets',
            int(api_params['max_inactive_duration'])
            if 'max_inactive_duration' in api_params
            else 1296000,  # One month
        )

    async def contract_transaction(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_transaction(
            'contract_history', api_params['address'], int(api_params['size'])
        )

    async def contract_internal_transaction(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_internal_transaction(
            'contract_history', api_params['address'], int(api_params['size'])
        )

    async def contract_stats(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_stats('contract_history', api_params['address'])

    async def contract_list(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_list('contract_history')
