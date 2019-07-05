import uvloop
import asyncio
import logging
from colorlog import ColoredFormatter

from tonga.models.structs.persistency_type import PersistencyType

from tonga.stores.local_store import LocalStore, StoreKeyNotFound


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


async def main() -> None:
    local_store = LocalStore(db_type=PersistencyType.MEMORY, loop=loop)
    print(loop)
    await local_store.__getattribute__('_build_set').__call__('test1', b'value1')
    await local_store.__getattribute__('_build_set').__call__('test2', b'value2')
    await local_store.__getattribute__('_build_set').__call__('test3', b'value3')

    await local_store.__getattribute__('_build_delete').__call__('test2')

    local_store.get_persistency().__getattribute__('_set_initialize').__call__()

    assert local_store.get_persistency().is_initialize()

    assert await local_store.get('test1') == b'value1'

    assert await local_store.get('test3') == b'value3'

    try:
        await local_store.delete('toto')
    except StoreKeyNotFound:
        print("Logic n'est ce pas ? ")

    await local_store.delete('test3')

    try:
        print('BEFORE THIS FUCKING LAST GET')
        await local_store.get('test3')
    except StoreKeyNotFound:
        print("2 Logic n'est ce pas ? ")
    loop.stop()

if __name__ == '__main__':
    logger = setup_logger()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    asyncio.ensure_future(main(), loop=loop)

    try:
        # Runs forever
        loop.run_forever()
    except Exception:
        # If an exception was raised loop was stopped
        loop.stop()
