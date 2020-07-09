import json
import traceback
from pathlib import Path
from pprint import pprint
from typing import Any, Collection, Dict, List, Optional, Set, Tuple, Union

import plyvel

from chainalytic_icon.common import config, util


class Storage(object):
    """
    Base class for different Storage implementations

    Properties:
        working_dir (str):
        storage_dir (str):
        transform_storage_dirs (dict):
        transform_storage_dbs (dict):
    
    Methods:
        api_call(api_id: str, api_params: dict) -> Optional[Any]

    """

    LAST_BLOCK_HEIGHT_KEY = b'last_block_height'

    LATEST_UNSTAKE_STATE_KEY = b'latest_unstake_state'
    LATEST_UNSTAKE_STATE_HEIGHT_KEY = b'latest_unstake_state_height'

    LATEST_STAKE_TOP100_KEY = b'latest_stake_top100'
    LATEST_STAKE_TOP100_HEIGHT_KEY = b'latest_stake_top100_height'

    RECENT_STAKE_WALLETS_KEY = b'recent_stake_wallets'
    RECENT_STAKE_WALLETS_HEIGHT_KEY = b'recent_stake_wallets_height'

    ABSTENTION_STAKE_KEY = b'abstention_stake'
    ABSTENTION_STAKE_HEIGHT_KEY = b'abstention_stake_height'

    FUNDED_WALLETS_HEIGHT_KEY = b'funded_wallets_height'
    MAX_FUNDED_WALLETS_LIST = 10000

    PASSIVE_STAKE_WALLETS_HEIGHT_KEY = b'passive_stake_wallets_height'
    MAX_PASSIVE_STAKE_WALLETS_LIST = 1000

    CONTRACT_HISTORY_HEIGHT_KEY = b'contract_history_height'
    CONTRACT_LIST_KEY = b'contract_list'

    def __init__(self, working_dir: str):
        super(Storage, self).__init__()
        self.working_dir = working_dir

        self.config = config.get_config(working_dir)
        self.storage_dir = (
            Path(working_dir, self.config['storage_dir'])
            .as_posix()
            .format(network_name=self.config['network_name'])
        )

        transforms = self.config['transforms']
        self.transform_storage_dirs = {
            tid: self.config['transform_storage_dir'].format(
                storage_dir=self.storage_dir, transform_id=tid
            )
            for tid in transforms
        }

        # Setup storage DB for all transforms
        for p in self.transform_storage_dirs.values():
            Path(p).parent.mkdir(parents=1, exist_ok=1)
        self.transform_storage_dbs = {
            tid: plyvel.DB(self.transform_storage_dirs[tid], create_if_missing=True)
            for tid in transforms
        }

        self.logger = util.get_child_logger('aggregator.storage')

    def call_storage(self, api_id: str, api_params: dict) -> Optional[Any]:
        func = getattr(self, api_id) if hasattr(self, api_id) else None

        if func:
            try:
                return func(api_params)
            except Exception as e:
                self.logger.error(f'{str(e)} \n {traceback.format_exc()}')
                return None
        else:
            self.logger.error(f'Storage API not implemented: {api_id}')
            return None

    # ##################################
    # Functions to be queried ( api_id )
    #
    def put_block(self, api_params: dict) -> bool:
        """Put block data to one specific transform storage.

        `last_block_height` value is also updated here
        """

        height: int = api_params['height']
        data: Union[Collection, bytes, str, float, int] = api_params['data']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        key = str(height).encode()

        if isinstance(data, dict):
            value = json.dumps(data).encode()
        elif isinstance(data, (list, tuple)):
            value = str(data).encode()
        elif isinstance(data, str):
            value = data.encode()
        elif isinstance(data, (int, float)):
            value = str(data).encode()
        elif isinstance(data, bytes):
            value = data
        else:
            return 0

        db.put(key, value)
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, key)

        return 1

    def get_block(self, api_params: dict) -> Optional[str]:
        """Get block data from one specific transform storage."""

        height: int = api_params['height']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        key = str(height).encode()
        value = db.get(key)
        value = value.decode() if value else value

        return value

    def last_block_height(self, api_params: dict) -> Optional[int]:
        """Get last block height in one specific transform storage."""

        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        value = db.get(Storage.LAST_BLOCK_HEIGHT_KEY)

        try:
            height = int(value)
        except Exception:
            height = None

        return height

    def set_last_block_height(self, api_params: dict) -> bool:
        """Set last block height in one specific transform storage."""

        height: int = api_params['height']
        transform_id: str = api_params['transform_id']

        try:
            height = int(height)
            value = str(height).encode()
        except Exception:
            return 0

        db = self.transform_storage_dbs[transform_id]
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, value)

        return 1

    ####################################
    # For `stake_history` transform only
    #
    def set_latest_unstake_state(self, api_params: dict) -> bool:
        unstake_state: dict = api_params['unstake_state']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if unstake_state['wallets'] is not None:
            db.put(Storage.LATEST_UNSTAKE_STATE_KEY, json.dumps(unstake_state['wallets']).encode())
        db.put(
            Storage.LATEST_UNSTAKE_STATE_HEIGHT_KEY, str(unstake_state['height']).encode(),
        )

        return 1

    def latest_unstake_state(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.LATEST_UNSTAKE_STATE_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.LATEST_UNSTAKE_STATE_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

    ###################################
    # For `stake_top100` transform only
    #
    def set_latest_stake_top100(self, api_params: dict) -> bool:
        stake_top100: dict = api_params['stake_top100']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if stake_top100['wallets'] is not None:
            db.put(Storage.LATEST_STAKE_TOP100_KEY, json.dumps(stake_top100['wallets']).encode())
        db.put(
            Storage.LATEST_STAKE_TOP100_HEIGHT_KEY, str(stake_top100['height']).encode(),
        )
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, str(stake_top100['height']).encode())

        return 1

    def latest_stake_top100(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.LATEST_STAKE_TOP100_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.LATEST_STAKE_TOP100_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

    ###########################################
    # For `recent_stake_wallets` transform only
    #
    def set_recent_stake_wallets(self, api_params: dict) -> bool:
        recent_stake_wallets: dict = api_params['recent_stake_wallets']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if recent_stake_wallets['wallets'] is not None:
            db.put(
                Storage.RECENT_STAKE_WALLETS_KEY,
                json.dumps(recent_stake_wallets['wallets']).encode(),
            )
        db.put(
            Storage.RECENT_STAKE_WALLETS_HEIGHT_KEY, str(recent_stake_wallets['height']).encode(),
        )
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, str(recent_stake_wallets['height']).encode())

        return 1

    def recent_stake_wallets(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.RECENT_STAKE_WALLETS_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.RECENT_STAKE_WALLETS_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

    #######################################
    # For `abstention_stake` transform only
    #
    def set_abstention_stake(self, api_params: dict) -> bool:
        abstention_stake: dict = api_params['abstention_stake']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if abstention_stake['wallets'] is not None:
            db.put(
                Storage.ABSTENTION_STAKE_KEY, json.dumps(abstention_stake['wallets']).encode(),
            )
        db.put(
            Storage.ABSTENTION_STAKE_HEIGHT_KEY, str(abstention_stake['height']).encode(),
        )
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, str(abstention_stake['height']).encode())

        return 1

    def abstention_stake(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.ABSTENTION_STAKE_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.ABSTENTION_STAKE_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

    #####################################
    # For `funded_wallets` transform only
    #
    def update_funded_wallets(self, api_params: dict) -> bool:
        updated_wallets: dict = api_params['updated_wallets']
        transform_id: str = api_params['transform_id']

        db_batch = self.transform_storage_dbs[transform_id].write_batch()
        for addr, balance in updated_wallets['wallets'].items():
            db_batch.put(addr.encode(), balance.encode())

        db_batch.put(Storage.FUNDED_WALLETS_HEIGHT_KEY, str(updated_wallets['height']).encode())
        db_batch.write()

        return 1

    def funded_wallets(self, api_params: dict) -> dict:
        min_balance: float = api_params['min_balance']
        transform_id: str = api_params['transform_id']

        wallets = {}
        db = self.transform_storage_dbs[transform_id]
        for addr, balance in db:
            if addr.startswith(b'hx') and float(balance) >= min_balance and float(balance) > 0:
                wallets[addr.decode()] = float(balance)

        wallets = {k: v for k, v in sorted(wallets.items(), key=lambda item: item[1], reverse=1)}
        total = len(wallets)
        wallets = {k: wallets[k] for k in list(wallets)[: Storage.MAX_FUNDED_WALLETS_LIST]}

        height = db.get(Storage.FUNDED_WALLETS_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height, 'total': total}

    ############################################
    # For `passive_stake_wallets` transform only
    #
    def update_passive_stake_wallets(self, api_params: dict) -> bool:
        updated_wallets: dict = api_params['updated_wallets']
        transform_id: str = api_params['transform_id']

        db_batch = self.transform_storage_dbs[transform_id].write_batch()
        for addr, balance in updated_wallets['wallets'].items():
            db_batch.put(addr.encode(), balance.encode())

        db_batch.put(
            Storage.PASSIVE_STAKE_WALLETS_HEIGHT_KEY, str(updated_wallets['height']).encode()
        )
        db_batch.write()

        return 1

    def passive_stake_wallets(self, api_params: dict) -> dict:
        max_inactive_duration: int = api_params['max_inactive_duration']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]

        latest_height = db.get(Storage.PASSIVE_STAKE_WALLETS_HEIGHT_KEY)
        latest_height = int(latest_height.decode()) if latest_height else None

        wallets = {}
        for addr, height in db:
            height = int(height)
            if addr.startswith(b'hx') and latest_height - height <= max_inactive_duration:
                wallets[addr.decode()] = f'{height}:{latest_height - height}'

        wallets = {
            k: v
            for k, v in sorted(
                wallets.items(), key=lambda item: int(item[1].split(':')[1]), reverse=1
            )
        }
        total = len(wallets)
        wallets = {k: wallets[k] for k in list(wallets)[: Storage.MAX_PASSIVE_STAKE_WALLETS_LIST]}

        return {'wallets': wallets, 'height': latest_height, 'total': total}

    #######################################
    # For `contract_history` transform only
    #
    def update_contract_history(self, api_params: dict) -> bool:
        updated_contract_state: dict = api_params['updated_contract_state']
        transform_id: str = api_params['transform_id']

        updated_contracts = updated_contract_state['updated_contracts']

        db = self.transform_storage_dbs[transform_id]
        db_batch = self.transform_storage_dbs[transform_id].write_batch()

        contract_list = db.get(Storage.CONTRACT_LIST_KEY)
        if contract_list:
            contract_list = json.loads(contract_list)
        else:
            contract_list = []

        for addr in updated_contracts:
            if addr not in contract_list:
                contract_list.append(addr)

            db_batch.put(addr.encode(), json.dumps(updated_contracts[addr]['stats']).encode())
            for i in updated_contracts[addr]['tx']:
                db_batch.put(
                    f'{addr}|tx|{i}'.encode(), json.dumps(updated_contracts[addr]['tx']).encode(),
                )
            for i in updated_contracts[addr]['internal_tx']:
                db_batch.put(
                    f'{addr}|internal_tx|{i}'.encode(),
                    json.dumps(updated_contracts[addr]['tx']).encode(),
                )

        db_batch.put(Storage.CONTRACT_LIST_KEY, json.dumps(contract_list).encode())

        db_batch.put(
            Storage.CONTRACT_HISTORY_HEIGHT_KEY, str(updated_contract_state['height']).encode()
        )

        db_batch.write()

        return 1

    def contract_transaction(self, api_params: dict) -> dict:
        address: str = api_params['address']
        size: int = api_params['size']  # Number of latest transactions
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]

        height = db.get(Storage.CONTRACT_HISTORY_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        stats = db.get(address.encode())
        if stats:
            stats = json.loads(stats)
        else:
            return {'transactions': [], 'height': height}

        latest_tx_id = stats['tx_count']

        txs = [
            json.loads(db.get(f'{address}|tx|{i}'.encode()))
            for i in range(latest_tx_id - size, latest_tx_id + 1)
        ]

        return {'transactions': txs, 'height': height}

    def contract_internal_transaction(self, api_params: dict) -> dict:
        address: str = api_params['address']
        size: int = api_params['size']  # Number of latest transactions
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]

        height = db.get(Storage.CONTRACT_HISTORY_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        stats = db.get(address.encode())
        if stats:
            stats = json.loads(stats)
        else:
            return {'internal_transaction': [], 'height': height}

        latest_tx_id = stats['internal_tx_count']

        txs = [
            json.loads(db.get(f'{address}|internal_tx|{i}'.encode()))
            for i in range(latest_tx_id - size, latest_tx_id + 1)
        ]

        return {'internal_transaction': txs, 'height': height}

    def contract_stats(self, api_params: dict) -> dict:
        address: str = api_params['address']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]

        height = db.get(Storage.CONTRACT_HISTORY_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        stats = db.get(address.encode())

        return {'stats': json.loads(stats) if stats else None, 'height': height}

    def contract_list(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]

        height = db.get(Storage.CONTRACT_HISTORY_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        contract_list = db.get(Storage.CONTRACT_LIST_KEY)

        return {
            'contract_list': json.loads(contract_list) if contract_list else None,
            'height': height,
        }
