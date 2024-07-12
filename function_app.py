import azure.functions as func
import logging
from services.auth_service import AuthService
from services.subscription_service import SubscriptionService
from services.graph_service import GraphService
import asyncio
from itertools import groupby
from operator import itemgetter
import collections
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

async def _appid_grouping_function(server_app_list):
    countuniquezone=server_app_list['uniquezone']
    sid_cid_data=server_app_list['data']
    if countuniquezone == None:
        return server_app_list
    elif len(countuniquezone) == 1:
        _groupby_parameter='appid'
        _sorted_sublist_serverlist=await _sort_server_list_function(sid_cid_data,_groupby_parameter)
        _groupby_on_appid=await _get_groupby(_sorted_sublist_serverlist,_groupby_parameter)
        #logging.info(_groupby_on_appid)
        _ci_server_list=[]
        _ai_server_list=[]
        _db_server_list=[]
        for elem in _groupby_on_appid:
            if elem[0]['appid'] == 'CI':
                _ci_server_list.extend(elem)
            elif elem[0]['appid'] == 'AI':
                _ai_server_list.extend(elem)
            else:
                _db_server_list.extend(elem)

        _groupby_appid_result_dict={
            'CI_Server':_ci_server_list,
            'AI_Server':_ai_server_list,
            'DB_Server':_db_server_list
            #'Message':'All systems in same zone'
        }
        return _groupby_appid_result_dict
    elif len(countuniquezone)==2:
        #logging.info(server_app_list)
        _groupby_parameter='appid'
        _sorted_sublist_serverlist=await _sort_server_list_function(sid_cid_data,_groupby_parameter)
        _groupby_on_appid=await _get_groupby(_sorted_sublist_serverlist,_groupby_parameter)
        #logging.info(_groupby_on_appid)
        _ci_server_list=[]
        _ai_server_list=[]
        _db_server_list=[]
        for elem in _groupby_on_appid:
            if elem[0]['appid'] == 'CI':
                _ci_server_list.extend(elem)
            elif elem[0]['appid'] == 'AI':
                _ai_server_list.extend(elem)
            else:
                _db_server_list.extend(elem)

        _groupby_appid_result_dict={
            'CI_Server':_ci_server_list,
            'AI_Server':_ai_server_list,
            'DB_Server':_db_server_list
            #'Message':'All systems in same zone'
        }
        return _groupby_appid_result_dict
        

async def _get_ci_ai_db_grouping(unique_zone_count_server):
    appid_grouping_result=[]
    appid_grouping_result=await asyncio.gather(
        *(asyncio.create_task(
            _appid_grouping_function(server_appid_list)
        )for server_appid_list in unique_zone_count_server)
    )
    return appid_grouping_result

async def _get_unique_zone_function(server):
    try:
        zones_count=collections.Counter(e['zones'][0] for e in server)
        unique_zone_dict={
            "uniquezone":zones_count,
            "data":server
        }
        return unique_zone_dict
    except Exception as e:
        non_zonal_deployment={
            "uniquezone":None,
            "data":server
        }
        logging.info(f"Non zonal")
        return non_zonal_deployment
    
async def _get_unique_zone_count(ci_ai_db_server_list):
    zone_count_result=[]
    zone_count_result=await asyncio.gather(
        *(
            asyncio.create_task(
                _get_unique_zone_function(server)
            )for server in ci_ai_db_server_list
        )
    )
    return zone_count_result

async def _sort_server_list_function(serverlistdata:list[dict],parameter:str):
    return sorted(serverlistdata,key=itemgetter(parameter))

async def _get_groupby(serverlistdata:list[dict],parameter:str):
    sid_cid_list=sorted(serverlistdata,key=itemgetter(parameter))
    result_groupby=[]
    for key,value in groupby(sid_cid_list,key=itemgetter(parameter)):
        sublist=[]
        for k in value:
            sublist.append(k)
        result_groupby.append(sublist)

    return result_groupby

async def _run_query_tenant(query:str,cred:str):
    credential,cloud=AuthService.get_credential(credential_key=cred)
    async with credential:
        subscriptions=await SubscriptionService.subscription_list(credentials=credential,cloud=cloud)
        sub_ids=SubscriptionService.filter_ids(subscriptions)
        server_list=await GraphService.run_query(
            query_str=query,
            credential=credential,
            sub_ids=sub_ids,
            cloud=cloud
        )
    server_tenant_dict={
        "credential_key":cred,
        "data":server_list
    }
    _groupby_parameter='SIDCID'
    _sorted_server_list=await _sort_server_list_function(server_tenant_dict['data'],_groupby_parameter)
    _groupby_on_sidcid=await _get_groupby(_sorted_server_list,_groupby_parameter)
    
    return _groupby_on_sidcid

async def _get_query_result(query:str):
    try:
        credential_list=AuthService.get_credential_keys()
        result=[]
        result=await asyncio.gather(
            *(asyncio.create_task(
                _run_query_tenant(query,cred)
            )for cred in credential_list)
        )
        return result
    except Exception as e:
        logging.error(f"Error fetching query {e}")
        return []

@app.route(route="http_trigger_zonal")
async def http_trigger_zonal(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    query=f"""resources 
    | where type == 'microsoft.compute/virtualmachines' 
    | where tags['comment'] != '' 
    | where resourceGroup startswith 'hec' 
    | extend LandscapeID = toupper(substring(resourceGroup,0,5)) 
    | extend CustomerID = toupper(substring(resourceGroup,6,3)) 
    | extend SID = tostring(split(tags['comment'], ' ')[1]) 
    | where strlen(SID) == 3 
    | where SID!= 'for' 
    | where tags['comment'] contains ' CI '  or tags['comment'] contains 'AI' or tags['comment'] contains ' DB ' 
    | extend nic_id= tostring(properties['networkProfile']['networkInterfaces'][0]['id']) 
    //| where SID == "HEA" and CustomerID == "NEE" or SID == "N3D" and CustomerID == "NEE" or SID == "SMD" and CustomerID == "CSD"
    //| where SID == "N3D" and CustomerID == "NEE"
    | project id,resourceGroup, name,CustomerID,SID,tags['comment'],LandscapeID,appid=case(tags['comment'] contains 'CI','CI',tags['comment'] contains 'DB','DB','AI'),SIDCID=strcat(SID,CustomerID),zones,tenantId"""
    
    ci_ai_db_server_list=await _get_query_result(query)
    unique_zone_count=await _get_unique_zone_count(ci_ai_db_server_list[0])
    ci_ai_db_grouping=await _get_ci_ai_db_grouping(unique_zone_count)
    return func.HttpResponse(
             str(ci_ai_db_grouping),
             status_code=200
        )