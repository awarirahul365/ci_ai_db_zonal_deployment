import logging
from time import time

from azure.core.credentials_async import AsyncTokenCredential
from azure.mgmt.resourcegraph.aio import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions
from msrestazure.azure_cloud import Cloud, AZURE_PUBLIC_CLOUD

from typing import Optional

from .subscription_service import SubscriptionService

class GraphService:

    @staticmethod
    async def run_query(
        query_str: str,
        credential: AsyncTokenCredential,
        sub_ids: Optional[list[str]] = None,
        cloud: Cloud = AZURE_PUBLIC_CLOUD
    ) -> list[object]:
        
        t0 = time()
        data = []
        async with credential:
            if sub_ids is None:
                sub_ids = await GraphService._get_sub_ids(credential, cloud)
            graph_client = ResourceGraphClient(
                credential,
                base_url=cloud.endpoints.resource_manager,
                credential_scopes=[cloud.endpoints.resource_manager+"/.default"]
            )
            query_request = QueryRequest(subscriptions=sub_ids, query=query_str)
            async with graph_client:
                query_response = await graph_client.resources(query_request)
                data += query_response.data
                while hasattr(query_response, "skip_token"):
                    skip_token = query_response.skip_token
                    if skip_token is None:
                        break
                    options = QueryRequestOptions(skip_token=skip_token)
                    query_request = QueryRequest(
                        subscriptions=sub_ids, 
                        query=query_str, 
                        options=options
                    )
                    query_response = await graph_client.resources(query_request)
                    data += query_response.data
        
            if data is None:
                data = []

        logging.info("Query took {:.3f} s".format(time()-t0))
        logging.info(f"We obtained a total of {len(data)} resources")
        
        return data


    @staticmethod
    async def _get_sub_ids(credential: AsyncTokenCredential, cloud: Cloud = AZURE_PUBLIC_CLOUD):
        
        subscriptions = await SubscriptionService.subscription_list(credential, cloud)
        sub_ids = SubscriptionService.filter_ids(subscription_list=subscriptions)

        return sub_ids