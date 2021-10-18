from datman.exceptions import UndefinedSetting


def key_exists_in_config(config, key):
    '''
    Searches for the existence of a given key in a
    given datman.config.config instance
    '''

    # Try at study level
    try:
        config.get_key(key)
    except UndefinedSetting:
        pass
    else:
        return True

    # Try at site level
    sites = []
    try:
        sites = config.study_config['Sites'].keys()
    except KeyError:
        print("No defined sites for study!")
        return False

    for s in sites:
        try:
            config.get_key(key, site=s)
        except UndefinedSetting:
            continue
        else:
            return True

    # No pings
    return False
