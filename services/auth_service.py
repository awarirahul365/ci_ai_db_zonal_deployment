import logging
import os
from typing import Tuple
from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import ChainedTokenCredential, AzureCliCredential, ManagedIdentityCredential, ClientSecretCredential
from shared_code.cloud_provider import get_cloud_provider
from msrestazure.azure_cloud import Cloud

class AuthService:
    '''Generic class for fetching credentials'''
    @staticmethod
    def get_default_credential() -> AsyncTokenCredential:
        '''returns Azure Credential object'''
        return ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential())
    
    @staticmethod
    def get_credential(credential_key) -> Tuple[ClientSecretCredential, Cloud]:
        '''Fetches credentials from configuration'''
        try:
            spn = os.getenv(credential_key, "").strip()
            if not spn:
                raise KeyError(f"'{spn}' key not found")

            spn_values = spn.split(',')
            cred_dict = {}
            for v in spn_values:
                pair = v.split(':')
                cred_dict[pair[0]] = pair[1]

            provider = cred_dict.get("provider", "default")
            cloud = get_cloud_provider(provider=provider)

            credential = ClientSecretCredential(
                tenant_id=cred_dict["tenantId"],
                client_id=cred_dict["clientId"],
                client_secret=cred_dict["clientSecret"],
                authority=cloud.endpoints.active_directory
            )

            return credential, cloud
    
        except Exception as e:
            logging.error(e)
            raise

    @staticmethod
    def get_credential_keys() -> list[str]:
        result = []
        credential_keys = os.getenv('CredentialKeys').split(',')
        for key in credential_keys:
            if not key:
                raise KeyError(f"Invalid credential key: '{key}'")
            
            result.append(key)

        return result