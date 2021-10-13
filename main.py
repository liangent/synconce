#!/usr/bin/env python3

import argparse
import configparser


def main(args):
    config = configparser.ConfigParser()
    config.read(args.config)

    import logging.config
    logging.config.fileConfig(config['global']['logging_conf'])
    import logging
    logger = logging.getLogger('synconce')

    from synconce import execute
    for section in config.sections():
        if section.startswith('sync_'):
            try:
                execute(config[section])
            except Exception:
                import traceback
                logger.error(traceback.format_exc())


if __name__ == '__main__':
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', required=True)

    args = parser.parse_args()

    main(args)
