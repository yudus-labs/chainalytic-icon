from typing import Dict, List, Optional, Set, Tuple

from chainalytic_icon.common import config

from . import api_bundle, collator


class Provider(object):
    """
    Properties:
        working_dir (str):
        zone_id (str):
        setting (dict):
        chain_registry (dict):
        collator (Collator):
        api_bundle (ApiBundle):

    """

    def __init__(self, working_dir: str):
        super(Provider, self).__init__()
        self.working_dir = working_dir

        config.set_working_dir(working_dir)
        self.config = config.get_config(working_dir)

        self.collator = collator.Collator(working_dir)
        self.api_bundle = api_bundle.ApiBundle(working_dir)
        self.api_bundle.set_collator(self.collator)
