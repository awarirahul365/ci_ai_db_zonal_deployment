from msrestazure.azure_cloud import Cloud, AZURE_PUBLIC_CLOUD as CLOUD_PUB, AZURE_CHINA_CLOUD as CLOUD_CH

china_provider = ["CN", "CHINA"]

def get_cloud_provider(provider: str) -> Cloud:

    cloud = CLOUD_PUB
    if provider in china_provider:
        cloud = CLOUD_CH
    
    return cloud