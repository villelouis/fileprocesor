import abc
from typing import List
from zipfile import ZIP_DEFLATED, ZipFile

import model


class Archiver(abc.ABC):
    EXTENSION = 'unknown'

    @abc.abstractmethod
    def zip(self, path: str, files_name_with_data: List[model.FileNameWithData]) -> None:
        pass

    @abc.abstractmethod
    def mem_unzip(self, path: str) -> List[str]:
        """
        unzip in memory
        """
        pass


class ZipArchiver(Archiver):
    EXTENSION = 'zip'

    def zip(self, path: str, files_name_with_data: List[model.FileNameWithData]) -> None:
        with ZipFile(f'{path}.zip', 'w', compression=ZIP_DEFLATED) as handle:
            for file_name_with_data in files_name_with_data:
                handle.writestr(file_name_with_data.file_name, file_name_with_data.data)

    def mem_unzip(self, path: str) -> List[str]:
        with ZipFile(path, 'r', compression=ZIP_DEFLATED) as handle:
            return [handle.read(name).decode() for name in handle.namelist()]
