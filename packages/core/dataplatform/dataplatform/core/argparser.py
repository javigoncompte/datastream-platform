from typing import List

from dataplatform.core.logger import get_logger

logger = get_logger(__name__)


def get_arguments(args: List[str]) -> List[str]:
    logger.info(f"ARGUMENTS: {args}")
    return args[1:]


def is_argument(args: List[str], arg: str) -> bool:
    is_argument = arg in args
    logger.info(f"ARGUMENT {arg}: {is_argument}")
    return is_argument


def is_not_argument(args: List[str], arg: str) -> bool:
    is_argument = arg not in args
    logger.info(f"ARGUMENT {arg}: {is_argument}")
    return is_argument
