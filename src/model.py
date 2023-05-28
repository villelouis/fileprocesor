import dataclasses
from collections import namedtuple
from typing import List


@dataclasses.dataclass
class ObjectDTO:
    name: str


@dataclasses.dataclass
class RootDTO:
    id: str
    level: int
    objects: List[ObjectDTO]


FileNameWithData = namedtuple('FileNameWithData', 'file_name data')
