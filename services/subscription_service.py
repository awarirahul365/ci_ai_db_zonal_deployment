from azure.mgmt.subscription.aio import SubscriptionClient
from azure.mgmt.subscription.models import Subscription
from msrestazure.azure_cloud import Cloud, AZURE_CHINA_CLOUD
from azure.core.credentials_async import AsyncTokenCredential

import logging

class SubscriptionService():
    """
    Class that manages subscription operations
    """
    
    def __init__(self):
        pass

    @staticmethod
    async def subscription_list(credentials: AsyncTokenCredential, cloud: Cloud) -> list[Subscription]:
        """Retrieves the list of subscriptions available on the tenant"""
        subs_client = SubscriptionClient(
            credential=credentials, 
            base_url=cloud.endpoints.resource_manager, 
            credential_scopes=[cloud.endpoints.resource_manager + '/.default']
        )

        results = []
        async with subs_client:
            subs_iterator = subs_client.subscriptions.list()
            results = [sub async for sub in subs_iterator]

        n_subs = len(results)
        logging.info(f"These credentials can access a total of {n_subs} subscriptions.")

        return results

    @staticmethod
    def filter_ids(subscription_list: list[Subscription]) -> list[str]:
        """Filters the Id from a list of subscriptions"""
        result = [sub.subscription_id for sub in subscription_list]
        return result