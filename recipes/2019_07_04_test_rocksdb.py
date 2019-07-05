import asyncio
import os
import logging
from colorlog import ColoredFormatter

from tonga.stores.persistency.shelve import ShelvePersistency, StoreKeyNotFound
from tonga.stores.persistency.memory import MemoryPersistency
from tonga.stores.persistency.rocksdb import RocksDBPersistency



def setup_logger():
    """Return a logger with a default ColoredFormatter."""
    formatter = ColoredFormatter(
        "%(log_color)s[%(asctime)s]%(levelname)s: %(name)s/%(module)s/%(funcName)s:%(lineno)d"
        " (%(thread)d) %(blue)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red',
        }
    )

    logger = logging.getLogger('tonga')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


async def main():
    print('start')
    for i in range(0, 1):
        print("Set toto -> b'titi'")
        await persistency.set('toto', b'titi')

        print("Get toto")
        r = await persistency.get('toto')
        print('return = ', r)

        print("Delete toto")
        await persistency.delete('toto')

        try:
            r = await persistency.get('toto')
            print('r = ', r)
        except StoreKeyNotFound:
            print('logic')


if __name__ == '__main__':
    logger = setup_logger()

    print('Start memory')
    persistency = MemoryPersistency()
    persistency.__getattribute__('_set_initialize').__call__()

    print(persistency.is_initialize())

    asyncio.run(main())

    print('Start shelve')
    persistency = ShelvePersistency(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                 'local_store.db'))
    persistency.__getattribute__('_set_initialize').__call__()

    asyncio.run(main())

    print('Start rocksDB')
    persistency = RocksDBPersistency(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                  'local_store'))
    persistency.__getattribute__('_set_initialize').__call__()

    asyncio.run(main())