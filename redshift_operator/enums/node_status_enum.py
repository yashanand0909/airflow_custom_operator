import enum


class NodeStatus(enum.Enum):
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    RUNNING = 'RUNNING'
