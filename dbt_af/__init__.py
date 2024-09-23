__all__ = [
    'dags',
    'conf',
]

# do not change the version, it's manged by release manager
__version__ = '0.8.0'

from . import conf, dags  # noqa
