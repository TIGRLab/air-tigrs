'''
Utilities for working with XNAT connections
'''
from datman.config import UndefinedSetting

def external_xnat_is_configured(config, site):
    try:
        config.get_key('XnatSourceCredential', site=site)
        config.get_key('XnatSource', site=site)
        config.get_key('XnatSourceArchive', site=site)
    except UndefinedSetting:
        return False
    else:
        return True
