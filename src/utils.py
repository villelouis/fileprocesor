import functools
import random
import time
import uuid

from src import model, logger


def get_random_object_dto() -> model.RootDTO:
    return model.RootDTO(
        id=str(uuid.uuid4()),
        level=random.randint(1, 100),
        objects=[model.ObjectDTO(name=str(uuid.uuid4())) for _ in range(random.randint(1, 10))]
    )


def timed(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        start = time.time()
        logger.log.info(f'stage {func.__name__} start')
        result = func(*args, **kwargs)
        end = time.time() - start
        logger.log.info(f'stage {func.__name__} completed! Consumed {round(end, 2)} sec')
        return result

    return wrapped
