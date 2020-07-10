import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chainalytic_icon.common import config, util


class ApiBundle(object):
    """
    The interface to external consumers/applications
    """

    def __init__(self, working_dir: str):
        super(ApiBundle, self).__init__()
        self.working_dir = working_dir
        self.collator = None

        self.logger = util.get_child_logger('provider.api_bundle')

    def set_collator(self, collator: 'Collator'):
        self.collator = collator

    async def call_api(self, api_id: str, api_params: dict) -> Dict:
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

    # #################
    # APIs to be called
    #
    async def last_block_height(self, api_params: dict) -> Optional[int]:
        if 'transform_id' in api_params:
            return await self.collator.last_block_height(api_params['transform_id'])

    async def latest_upstream_block_height(self, api_params: dict) -> Optional[int]:
        return await self.collator.latest_upstream_block_height()

    async def contract_transaction(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_transaction(
            api_params['address'], int(api_params['size'])
        )

    async def contract_internal_transaction(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_internal_transaction(
            api_params['address'], int(api_params['size'])
        )

    async def contract_stats(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_stats(api_params['address'])

    async def contract_list(self, api_params: dict) -> Optional[dict]:
        return await self.collator.contract_list()
