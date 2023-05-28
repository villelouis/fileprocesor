import random
import uuid

import model


def get_random_object_dto() -> model.RootDTO:
    return model.RootDTO(
        id=str(uuid.uuid4()),
        level=random.randint(0, 100),
        objects=[model.ObjectDTO(name=str(uuid.uuid4())) for _ in range(random.randint(1, 10))]
    )