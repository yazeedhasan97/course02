from enum import Enum

ALL = "ALL"

class Status(Enum):
    SUCCESS = 1
    RUNNING = 0
    FAILED = -1
    SKIPPED = -100
