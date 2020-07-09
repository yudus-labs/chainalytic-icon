import argparse
import asyncio
import traceback
from typing import Dict, List, Optional, Set, Tuple

import websockets
from jsonrpcclient.clients.http_client import HTTPClient
from jsonrpcclient.clients.websockets_client import WebSocketsClient

SUCCEED_STATUS = 1
FAILED_STATUS = 0


async def call_ws(endpoint: str, **kwargs) -> Dict:
    """Use this function to communicate with Aggregator services

    Default service endpoints:
        Aggregator: localhost:5500
    
    Returns:
        dict: {'status': bool, 'data': Any}
    """
    try:
        async with websockets.connect(f"ws://{endpoint}") as ws:
            r = await WebSocketsClient(ws).request("_call", **kwargs)
        return {'status': SUCCEED_STATUS, 'data': r.data.result}
    except Exception as e:
        return {'status': FAILED_STATUS, 'data': f'{str(e)}\n{traceback.format_exc()}'}


def call(endpoint: str, **kwargs) -> Dict:
    """
    Synchronous version of `call_ws()`
    """
    return asyncio.get_event_loop().run_until_complete(call_ws(endpoint, **kwargs))


def call_aiohttp(endpoint: str, **kwargs) -> Dict:
    """Use this function to communicate with Provider services

    Default service endpoints:
        Provider: localhost:5600
    
    Returns:
        dict: {'status': bool, 'data': Any}
    """
    try:
        client = HTTPClient(f'http://{endpoint}')
        r = client.request("_call", **kwargs)
        return {'status': SUCCEED_STATUS, 'data': r.data.result}
    except Exception as e:
        return {'status': FAILED_STATUS, 'data': f'{str(e)}\n{traceback.format_exc()}'}
