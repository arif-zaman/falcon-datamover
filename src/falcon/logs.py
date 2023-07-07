import logging as logger
from falcon.configs import configurations


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
if configurations["loglevel"] == "debug":
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.DEBUG,
    )
else:
    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logger.INFO
    )
