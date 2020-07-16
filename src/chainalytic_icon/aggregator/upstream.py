import functools
import json
import traceback
from pathlib import Path
from pprint import pprint
from time import time
from typing import Dict, List, Optional, Set, Tuple

import plyvel
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider

from chainalytic_icon.common import config, util

BLOCK_HEIGHT_KEY = b'block_height_key'
BLOCK_HEIGHT_BYTES_LEN = 12


def handle_client_failure(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            http_provider = HTTPProvider(args[0].loopchain_node_endpoint, 3)

            if args[0].direct_db_access:
                return func(*args, **kwargs)
            elif http_provider.is_connected():
                if args[0].icon_service is None:
                    args[0].icon_service = IconService(http_provider)
                return func(*args, **kwargs)
            else:
                args[0].logger.warning(
                    f'Citizen node is not connected: {args[0].loopchain_node_endpoint}'
                )
                return None

        except Exception as e:
            args[0].icon_service = None
            args[0].logger.error('handle_client_failure(): Failed to setup icon_service')
            args[0].logger.error(str(e))
            return None

    return wrapper


def handle_unknown_failure(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            args[0].logger.error(f'handle_unknown_failure(): There is error while calling: {func}')
            args[0].logger.error(str(e))
            args[0].logger.error(traceback.format_exc())
            return None

    return wrapper


class Upstream(object):
    LAST_BLOCK_KEY = b'last_block_key'

    def __init__(self, working_dir: str):
        super(Upstream, self).__init__()

        self.working_dir = working_dir
        self.config = config.get_config(working_dir)
        self.logger = util.get_child_logger('aggregator.upstream')

        self.direct_db_access = self.config['direct_db_access']
        self.chain_db_dir = self.config['chain_db_dir'] if self.config else ''
        self.score_db_icondex_dir = self.config['score_db_icondex_dir'] if self.config else ''

        self.loopchain_node_endpoint = self.config['loopchain_node_endpoint'] if self.config else ''
        if not self.loopchain_node_endpoint.startswith('http'):
            self.loopchain_node_endpoint = f'http://{self.loopchain_node_endpoint}'

        if self.direct_db_access:
            assert Path(self.chain_db_dir).exists(), f'Chain DB does not exist: {self.chain_db_dir}'
            self.chain_db = plyvel.DB(self.chain_db_dir)
            self.score_db_icondex_db = plyvel.DB(self.score_db_icondex_dir)
        else:
            self.icon_service = None

    @handle_client_failure
    def _get_total_supply(self):
        if self.direct_db_access:
            r = self.score_db_icondex_db.get(b'total_supply')
            return int.from_bytes(r, 'big') / 10 ** 18
        else:
            return self.icon_service.get_total_supply() / 10 ** 18

    @handle_client_failure
    def _get_block(self, height: int) -> Optional[Dict]:
        if self.direct_db_access:
            heightkey = BLOCK_HEIGHT_KEY + height.to_bytes(BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
            block_hash = self.chain_db.get(heightkey)

            if not block_hash:
                return None

            data = self.chain_db.get(block_hash)
            try:
                return json.loads(data)
            except Exception as e:
                self.logger.error(f'_get_block(): Failed to read block from LevelDB: {height}')
                self.logger.error(str(e))
                return None
        else:
            try:
                last_block = self._icon_service_get_last_block()
                if height <= last_block:
                    return self.icon_service.get_block(height)
                else:
                    return -1
            except Exception as e:
                self.logger.error(f'_get_block(): icon_service failed to get_block: {height}')
                self.logger.error(str(e))
                return None

    @handle_client_failure
    def _get_tx_result(self, tx_hash: str) -> Optional[Dict]:
        if self.direct_db_access:
            try:
                tx_detail = self.chain_db.get(tx_hash.encode())

                if not tx_detail:
                    return None
                else:
                    tx_detail = json.loads(tx_detail)
                    return tx_detail['result'] if 'result' in tx_detail else None

            except Exception as e:
                self.logger.error(f'_get_tx_result(): Failed to read Tx from LevelDB: {tx_hash}')
                self.logger.error(str(e))
                return None
        else:
            try:
                tx_result = self.icon_service.get_transaction_result(tx_hash)
                return tx_result

            except Exception as e:
                self.logger.error(
                    f'_get_tx_result(): icon_service failed to get_transaction: {tx_hash}'
                )
                self.logger.error(str(e))
                return None

    @handle_client_failure
    def _icon_service_get_last_block(self):
        return self.icon_service.get_block('latest')['height']

    def _parse_block(self, block: dict) -> tuple:
        txs = (
            block['confirmed_transaction_list']
            if 'confirmed_transaction_list' in block
            else block['transactions']
        )
        if 'timestamp' in block:
            timestamp = (
                int(block['timestamp'], 16)
                if isinstance(block['timestamp'], str)
                else block['timestamp']
            )
        else:
            timestamp = block['time_stamp']

        return (txs, timestamp)

    def _get_block_fund_transfer_tx(self, height: int) -> Optional[dict]:
        """Filter out and process ICX transfering txs."""
        self.logger.debug(f'Feeding block: {height}')

        block = self._get_block(height)
        if block is None:
            self.logger.warning(f'Block {height} not found')
            return None
        elif block == -1:
            return -1

        try:
            txs, timestamp = self._parse_block(block)
        except Exception as e:
            self.logger.error('ERROR in block data loading, skipped feeding')
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        try:
            fund_transfer_txs = []
            for tx in txs:
                if 'data' in tx:
                    continue
                try:
                    tx_data = {}
                    tx_data['from'] = tx['from']
                    tx_data['to'] = tx['to']
                    tx_data['value'] = (
                        int(tx['value'], 16) / 10 ** 18
                        if self.direct_db_access
                        else tx['value'] / 10 ** 18
                    )
                    fund_transfer_txs.append(tx_data)
                except (ValueError, KeyError):
                    self.logger.warning('There is issue in fund transfer transaction:')
                    self.logger.warning(util.pretty(tx))

        except Exception as e:
            self.logger.error('ERROR in data pre-processing')
            self.logger.error('Source TX data:')
            self.logger.error(util.pretty(tx))
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        return {
            'data': fund_transfer_txs,
            'timestamp': timestamp,
        }

    def _get_block_stake_tx(self, height: int) -> Optional[dict]:
        """Filter out and process `setStake` txs."""
        self.logger.debug(f'Feeding block: {height}')

        block = self._get_block(height)
        if block is None:
            self.logger.warning(f'Block {height} not found')
            return None
        elif block == -1:
            return -1

        try:
            txs, timestamp = self._parse_block(block)
        except Exception as e:
            self.logger.error('ERROR in block data loading, skipped feeding')
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        try:
            set_stake_wallets = {}
            for tx in txs:
                if 'data' not in tx:
                    continue
                if 'method' not in tx['data']:
                    continue
                if tx['data']['method'] == 'setStake':
                    try:
                        stake_value = int(tx['data']['params']['value'], 16) / 10 ** 18
                        set_stake_wallets[tx["from"]] = stake_value
                    except (ValueError, KeyError):
                        self.logger.warning('There is issue in setStake transaction:')
                        self.logger.warning(util.pretty(tx))

        except Exception as e:
            self.logger.error('ERROR in data pre-processing')
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        return {
            'data': set_stake_wallets,
            'timestamp': timestamp,
            'total_supply': self._get_total_supply(),
        }

    def _get_block_stake_delegation_tx(self, height: int) -> Optional[dict]:
        """Filter out and process `setStake` and `setDelegation` txs."""
        self.logger.debug(f'Feeding block: {height}')

        block = self._get_block(height)
        if block is None:
            self.logger.warning(f'Block {height} not found')
            return None
        elif block == -1:
            return -1

        try:
            txs, timestamp = self._parse_block(block)
        except Exception as e:
            self.logger.error('ERROR in block data loading, skipped feeding')
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        try:
            set_stake_wallets = {}
            set_delegation_wallets = {}
            for tx in txs:
                if 'data' not in tx:
                    continue
                if 'method' not in tx['data']:
                    continue
                if tx['data']['method'] == 'setStake':
                    try:
                        stake_value = int(tx['data']['params']['value'], 16) / 10 ** 18
                        set_stake_wallets[tx["from"]] = stake_value
                    except (ValueError, KeyError):
                        self.logger.warning('There is issue in setStake transaction:')
                        self.logger.warning(util.pretty(tx))

                elif tx['data']['method'] == 'setDelegation':
                    try:
                        set_delegation_wallets[tx["from"]] = tx['data']['params']['delegations']
                    except KeyError:
                        self.logger.warning('There is issue in setDelegation transaction:')
                        self.logger.warning(util.pretty(tx))

        except Exception as e:
            self.logger.error('ERROR in data pre-processing')
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        return {
            'data': {'stake': set_stake_wallets, 'delegation': set_delegation_wallets},
            'timestamp': timestamp,
            'total_supply': self._get_total_supply(),
        }

    def _get_block_contract_tx(self, height: int) -> Optional[dict]:
        """Filter out contract txs."""
        self.logger.debug(f'Feeding block: {height}')

        block = self._get_block(height)
        if block is None:
            self.logger.warning(f'Block {height} not found')
            return None
        elif block == -1:
            return -1

        try:
            txs, timestamp = self._parse_block(block)
        except Exception as e:
            self.logger.error('ERROR in block data loading, skipped feeding')
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        try:
            contract_txs = []
            for tx in txs:
                if 'to' in tx:
                    if tx['to'].startswith('cx'):
                        # pprint(tx)
                        tx_data = {
                            'status': None,
                            'contract_address': tx['to'],
                            'timestamp': int(tx['timestamp'], 16)
                            if isinstance(tx['timestamp'], str)
                            else tx['timestamp'],
                            'hash': tx['txHash'],
                            'from': tx['from'],
                            'value': None,
                            'fee': None,
                            'internal': [],
                        }
                        if 'value' in tx:
                            tx_data['value'] = (
                                int(tx['value'], 16)
                                if isinstance(tx['value'], str)
                                else tx['value']
                            ) / 10 ** 18

                        tx_result = self._get_tx_result(tx['txHash'])
                        # pprint(tx_result)
                        tx_data['fee'] = (
                            (
                                int(tx_result['stepPrice'], 16)
                                if isinstance(tx_result['stepPrice'], str)
                                else tx_result['stepPrice']
                            )
                            * (
                                int(tx_result['stepUsed'], 16)
                                if isinstance(tx_result['stepUsed'], str)
                                else tx_result['stepUsed']
                            )
                            / 10 ** 18
                        )

                        tx_data['status'] = (
                            int(tx_result['status'], 16)
                            if isinstance(tx_result['status'], str)
                            else tx_result['status']
                        )

                        for event in tx_result['eventLogs']:
                            if event['indexed'][0].startswith('ICXTransfer'):
                                internal = {}
                                internal['itx_target'] = event['indexed'][2]
                                internal['itx_value'] = int(event['indexed'][3], 16) / 10 ** 18
                                tx_data['internal'].append(internal)

                        contract_txs.append(tx_data)

        except Exception as e:
            self.logger.error('ERROR in data pre-processing')
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            return None

        return {
            'data': contract_txs,
            'timestamp': timestamp,
            'total_supply': self._get_total_supply(),
        }

    @handle_unknown_failure
    async def get_block(self, height: int, transform_id: str) -> Optional[dict]:
        if transform_id == 'stake_history':
            return self._get_block_stake_tx(height)
        elif transform_id == 'stake_top100':
            return self._get_block_stake_tx(height)
        elif transform_id == 'recent_stake_wallets':
            return self._get_block_stake_tx(height)
        elif transform_id == 'abstention_stake':
            return self._get_block_stake_delegation_tx(height)
        elif transform_id == 'funded_wallets':
            return self._get_block_fund_transfer_tx(height)
        elif transform_id == 'passive_stake_wallets':
            return self._get_block_stake_delegation_tx(height)
        elif transform_id == 'contract_history':
            return self._get_block_contract_tx(height)

    @handle_unknown_failure
    async def last_block_height(self) -> Optional[int]:
        """Get last block height from chain
        """

        if self.direct_db_access:
            block_hash = self.chain_db.get(Upstream.LAST_BLOCK_KEY)
            data = self.chain_db.get(block_hash)
            if data:
                block = json.loads(data)
                return int(block['height'], 16)
        else:
            return self._icon_service_get_last_block()
