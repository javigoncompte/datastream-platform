import logging


def get_logger(name: str = __name__):
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s-%(relativeCreated)d %(levelname)s %(filename)s "
        "%(funcName)s:%(lineno)s : %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    return logger


def hello_world():
    """Simple hello world function for testing imports."""
    logger = get_logger(__name__)
    logger.info("Hello, world!")
    print("Hello, world! new version")
