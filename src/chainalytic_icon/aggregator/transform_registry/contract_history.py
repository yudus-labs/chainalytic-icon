import json
import time
from typing import Dict, List, Optional, Set, Tuple, Union

from iconservice.icon_config import default_icon_config
from iconservice.icon_constant import ConfigKey
from iconservice.iiss.engine import Engine

from chainalytic_icon.aggregator.transform import BaseTransform


class Transform(BaseTransform):
    START_BLOCK_HEIGHT = 1  # This is used in Aggregator service initialization

    def __init__(self, working_dir: str, transform_id: str):
        super(Transform, self).__init__(working_dir, transform_id)

    async def execute(self, height: int, input_data: dict) -> Optional[Dict]:
        self.logger.debug(f'Executing: {self.transform_id}, height: {height}')

        cache_db = self.transform_cache_db
        cache_db_batch = self.transform_cache_db.write_batch()

        if not self.ensure_block_height_match(height):
            return self.load_last_output()

        # #################################################

        contract_txs = input_data['data']
        updated_contracts = {}

        for tx in contract_txs:
            addr = tx['contract_address']
            if addr not in updated_contracts:
                updated_contracts[addr] = {
                    'stats': {},
                    'tx': {},
                    'itx': {},
                }

            if not updated_contracts[addr]['stats']:
                updated_contracts[addr]['stats'] = cache_db.get(addr.encode())
                if not updated_contracts[addr]['stats']:
                    updated_contracts[addr]['stats'] = {
                        'tx_volume': 0,
                        'tx_count': 0,
                        'itx_volume': 0,
                        'itx_count': 0,
                    }
                else:
                    updated_contracts[addr]['stats'] = json.loads(updated_contracts[addr]['stats'])

            # Internal txs
            if tx['internal']:
                for internal in tx['internal']:
                    updated_contracts[addr]['stats']['itx_count'] += 1
                    next_tx_id = updated_contracts[addr]['stats']['itx_count']
                    updated_contracts[addr]['itx'][f'{next_tx_id}'] = {
                        'status': tx['status'],
                        'height': height,
                        'timestamp': tx['timestamp'],
                        'hash': tx['hash'] if tx['hash'].startswith('0x') else f"0x{tx['hash']}",
                        'value': tx['value'],
                        'fee': tx['fee'],
                        'itx_target': internal['itx_target'],
                        'itx_value': internal['itx_value'],
                    }
                    if internal['itx_value'] and tx['status']:
                        updated_contracts[addr]['stats']['itx_volume'] += internal['itx_value']

            # All txs
            updated_contracts[addr]['stats']['tx_count'] += 1
            next_tx_id = updated_contracts[addr]['stats']['tx_count']
            updated_contracts[addr]['tx'][f'{next_tx_id}'] = {
                'status': tx['status'],
                'height': height,
                'timestamp': tx['timestamp'],
                'hash': tx['hash'] if tx['hash'].startswith('0x') else f"0x{tx['hash']}",
                'from': tx['from'],
                'value': tx['value'],
                'fee': tx['fee'],
            }
            if tx['value'] and tx['status']:
                updated_contracts[addr]['stats']['tx_volume'] += tx['value']

            cache_db_batch.put(addr.encode(), json.dumps(updated_contracts[addr]['stats']).encode())

        cache_db_batch.put(Transform.LAST_STATE_HEIGHT_KEY, str(height).encode())

        output = {
            'height': height,
            'block_data': {},
            'latest_state_data': {
                'updated_contract_state': {'updated_contracts': updated_contracts, 'height': height}
            },
        }

        self.save_last_output(cache_db_batch, output)

        cache_db_batch.write()

        self.logger.debug(f'Executed: {self.transform_id}, height: {height}')

        return output
