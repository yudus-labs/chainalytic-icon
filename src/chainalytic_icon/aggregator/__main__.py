import argparse
import asyncio
import os
import sys
import time

import websockets
from jsonrpcclient.clients.websockets_client import WebSocketsClient
from jsonrpcserver import method

from chainalytic_icon.common import config, rpc_client, rpc_server
from chainalytic_icon.common.rpc_server import EXIT_SERVICE, main_dispatcher, show_call_info
from chainalytic_icon.common.util import create_logger
from chainalytic_icon.cli.console import Console

from . import kernel, upstream, storage

_KERNEL = None
_STORAGE = None
_UPSTREAM = None

_LOGGER = None


@method
async def _call(call_id: str, **kwargs):
    params = kwargs
    show_call_info(call_id, params)

    if call_id == 'ping':
        message = '\n'.join(
            [
                'Pong !',
                'Aggregator service is running',
                f'Working dir: {_KERNEL.working_dir}',
                f'Params: {params}',
            ]
        )
        return message
    elif call_id == 'exit':
        return EXIT_SERVICE
    elif call_id == 'ls_all_transform_id':
        return list(_KERNEL.transforms)

    elif call_id == 'latest_upstream_block_height':
        return await _UPSTREAM.last_block_height()

    elif call_id == 'call_storage':
        api_id = params['api_id']
        api_params = params['api_params']
        return await _STORAGE.call_storage(api_id, api_params)

    else:
        return 'Not implemented'


def initialize():
    _LOGGER.info('Initializing Aggregator service...')

    # Set last_block_height value for the first time
    for tid in _KERNEL.transforms:
        height = _STORAGE.last_block_height(api_params={'transform_id': tid},)
        if not height:
            _STORAGE.set_last_block_height(
                api_params={
                    'height': _KERNEL.transforms[tid].START_BLOCK_HEIGHT - 1,
                    'transform_id': tid,
                },
            )
            _LOGGER.info(f'--Set initial last_block_height for transform: {tid}')
    _LOGGER.info('Initialized Aggregator service')
    _LOGGER.info('')


async def aggregate_data():
    while 1:
        _LOGGER.info('New aggregation')

        # A temp hack, without it websocket server won't work, still an unknown reason
        if _UPSTREAM.direct_db_access:
            await asyncio.sleep(0.0001)

        # A temp hack, for better coroutine scheduling
        else:
            await asyncio.sleep(0.05)

        t = time.time()
        for tid in _KERNEL.transforms:
            t1 = time.time()
            _LOGGER.info(f'Transform ID: {tid}')
            _LOGGER.debug(f'--Trying to fetch data...')

            last_block_height = _STORAGE.last_block_height(api_params={'transform_id': tid},)
            _LOGGER.debug(f'----Last block height: {last_block_height}')

            if type(last_block_height) == int:
                next_block_height = last_block_height + 1
                block_data = await _UPSTREAM.get_block(height=next_block_height, transform_id=tid,)
                if block_data:
                    _LOGGER.debug(f'--Fetched data successfully')
                    _LOGGER.debug(f'--Next block height: {next_block_height}')
                    _LOGGER.debug(f'--Preparing to execute next block...')
                    await _KERNEL.execute(
                        height=next_block_height, input_data=block_data, transform_id=tid,
                    )
                    _LOGGER.debug(f'--Executed block {next_block_height} successfully')

            agg_time = round(time.time() - t1, 4)
            _LOGGER.info(f'--Aggregated block {next_block_height} in {agg_time}s')

        _LOGGER.debug('----')
        tagg_time = round(time.time() - t, 4)
        _LOGGER.debug(f'Total aggregation time: {tagg_time}s')
        _LOGGER.debug(f'Estimated aggregation speed: {int(1/tagg_time)} blocks/s')
        _LOGGER.info('')
        _LOGGER.info('')


def _run_server(endpoint, working_dir):
    global _UPSTREAM
    global _STORAGE
    global _KERNEL
    global _LOGGER
    _LOGGER = create_logger(working_dir, 'aggregator')
    rpc_server.set_logger(_LOGGER)

    _UPSTREAM = upstream.Upstream(working_dir)
    _STORAGE = storage.Storage(working_dir)
    _KERNEL = kernel.Kernel(working_dir, _STORAGE)

    _LOGGER.info(f'Aggregator endpoint: {endpoint}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])

    initialize()
    asyncio.get_event_loop().create_task(aggregate_data())

    start_server = websockets.serve(main_dispatcher, host, port)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
    _LOGGER.info('Exited Aggregator')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Chainalytic Aggregator server')
    parser.add_argument('--endpoint', type=str, help='Endpoint of Aggregator server')
    parser.add_argument('--working_dir', type=str, help='Current working directory')
    args = parser.parse_args()
    endpoint = args.endpoint
    working_dir = args.working_dir if args.working_dir != '.' else os.getcwd()
    _run_server(endpoint, working_dir)
