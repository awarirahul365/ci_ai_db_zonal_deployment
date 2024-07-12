from typing import Any, Optional

import json
import asyncio
import requests
import logging


def list_to_chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def gather_with_concurrency(n: int, *tasks):
    """Limits tasks concurrency to n sized chunks"""
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task
    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def post_message(url: str, payload: Any):
    '''Post request'''
    if not url:
        logging.info('Notification url was not defined.')
        return None

    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    logging.info(f'Post alert returned {response.text}')
    return response


def extract_string(full_string: str, start_string:str, end_string: Optional[str] = None):
    """Extracts the string between start_string and end_string"""

    name      = None
    end_index = -1
    start_index = full_string.find(start_string)
    if start_index > -1:
        start_index += len(start_string)
    else:
        start_index = 0

    if end_string is not None:
        end_index = full_string.find(end_string, start_index)
        if end_index > -1:
            name = full_string[start_index:end_index]
    else:
        name = full_string[start_index:]

    return name


def get_resource_value(resource_uri: str, resource_name: str):
    """Gets the resource name based on resource type
    Function that returns the name of a resource from resource id/uri based on
    resource type name.
    Args:
        resource_uri (string): resource id/uri
        resource_name (string): Name of the resource type, e.g. capacityPools
    Returns:
        string: Returns the resource name
    """

    if not resource_uri.strip():
        return None

    if not resource_name.startswith('/'):
        resource_name = '/{}'.format(resource_name)

    if not resource_uri.startswith('/'):
        resource_uri = '/{}'.format(resource_uri)

    # Checks to see if the ResourceName and ResourceGroup is the same name and
    # if so handles it specially.
    rg_resource_name = '/resourceGroups{}'.format(resource_name)
    rg_index = resource_uri.lower().find(rg_resource_name.lower())
    # dealing with case where resource name is the same as resource group
    if rg_index > -1:
        removed_same_rg_name = resource_uri.lower().split(
            resource_name.lower())[-1]
        return removed_same_rg_name.split('/')[1]

    index = resource_uri.lower().find(resource_name.lower())
    if index > -1:
        res = resource_uri[index + len(resource_name):].split('/')
        if len(res) > 1:
            return res[1]

    return None