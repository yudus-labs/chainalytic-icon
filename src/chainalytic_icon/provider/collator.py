import json
from pprint import pprint
from typing import Dict, List, Optional, Set, Tuple, Union

from chainalytic_icon.common import config, util, rpc_client


class Collator(object):
    def __init__(self, working_dir: str):
        super(Collator, self).__init__()
        self.working_dir = working_dir
        self.aggregator_endpoint = config.get_config(working_dir)['aggregator_endpoint']

        self.logger = util.get_child_logger('provider.collator')

    async def get_block(
        self, height: int, transform_id: str
    ) -> Optional[Union[Dict, str, float, int, bytes]]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='get_block',
            api_params={'height': height, 'transform_id': transform_id},
        )
        if r['status'] and r['data']:
            try:
                return json.loads(r['data'])
            except:
                return None
        else:
            return None

    async def last_block_height(self, transform_id: str) -> Optional[int]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='last_block_height',
            api_params={'transform_id': transform_id},
        )
        if r['status'] and r['data']:
            try:
                return int(r['data'])
            except:
                return None
        else:
            return None

    async def latest_upstream_block_height(self) -> Optional[int]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint, call_id='latest_upstream_block_height',
        )
        if r['status'] and r['data']:
            try:
                return int(r['data'])
            except:
                return None
        else:
            return None

    ####################################
    # For `stake_history` transform only
    #
    async def latest_unstake_state(self) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='latest_unstake_state',
            api_params={'transform_id': 'stake_history'},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    ####################################
    # For `stake_top100` transform only
    #
    async def latest_stake_top100(self) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='latest_stake_top100',
            api_params={'transform_id': 'stake_top100'},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    ###########################################
    # For `recent_stake_wallets` transform only
    #
    async def recent_stake_wallets(self) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='recent_stake_wallets',
            api_params={'transform_id': 'recent_stake_wallets'},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    #######################################
    # For `abstention_stake` transform only
    #
    async def abstention_stake(self) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='abstention_stake',
            api_params={'transform_id': 'abstention_stake'},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    #####################################
    # For `funded_wallets` transform only
    #
    async def funded_wallets(self, min_balance: float) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='funded_wallets',
            api_params={'transform_id': 'funded_wallets', 'min_balance': min_balance},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    ############################################
    # For `passive_stake_wallets` transform only
    #
    async def passive_stake_wallets(self, max_inactive_duration: int) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='passive_stake_wallets',
            api_params={
                'transform_id': 'passive_stake_wallets',
                'max_inactive_duration': max_inactive_duration,
            },
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    #######################################
    # For `contract_history` transform only
    #
    async def contract_transaction(self, address: str, size: int) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='contract_transaction',
            api_params={'transform_id': 'contract_history', 'address': address, 'size': size},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    async def contract_internal_transaction(self, address: str, size: int) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='contract_internal_transaction',
            api_params={'transform_id': 'contract_history', 'address': address, 'size': size},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    async def contract_stats(self, address: str) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='contract_stats',
            api_params={'transform_id': 'contract_history', 'address': address},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None

    async def contract_list(self,) -> Optional[dict]:
        r = await rpc_client.call_ws(
            self.aggregator_endpoint,
            call_id='call_storage',
            api_id='contract_list',
            api_params={'transform_id': 'contract_history'},
        )
        if r['status']:
            return r['data']
        else:
            self.logger.error('Failed to request data from Aggregator')
            self.logger.error(r['data'])
            return None
