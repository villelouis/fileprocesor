import abc
import csv
import os
import time
from concurrent.futures import as_completed, Future, wait, _base
from typing import List, Optional

from src import model, settings


class Stage(abc.ABC):

    @abc.abstractmethod
    def run(self, futures: List[Future]) -> Optional[List[Future]]:
        pass


class FileWriter(abc.ABC):
    @abc.abstractmethod
    def __init__(self, folder: str, id_level_file_name: str, id_object_file_name: str):
        pass


class WithPoolExecutor:

    def __init__(self, worker_pool=None):
        self.worker_pool: _base.Executor = worker_pool


class OutputStage(Stage, WithPoolExecutor, FileWriter, abc.ABC):
    def __init__(self, worker_pool, folder: str, id_level_file_name: str = settings.ID_LEVEL_CSV_NAME,
                 id_object_file_name: str = settings.ID_OBJECT_NAME_CSV_NAME):
        super().__init__(worker_pool=worker_pool)
        self.folder = folder
        self.id_level_file_name = id_level_file_name
        self.id_object_file_name = id_object_file_name


def csv_write_task(folder: str, output_file: str, raws_list: List[List[str]]):
    with open(os.path.join(folder, output_file), 'w') as file:
        csv.writer(file).writerows(raws_list)


class CSVWithPoolExecutorOutputStage(OutputStage):

    def run(self, futures: List[Future]) -> Optional[List[Future]]:
        os.makedirs(self.folder, exist_ok=True)
        id_levels = []
        id_objects = []
        for future in as_completed(futures):
            root_dto: model.RootDTO = future.result()
            id_levels.append([root_dto.id, str(root_dto.level)])
            for object_dto in root_dto.objects:
                id_objects.append([root_dto.id, object_dto.name])

        futures = [
            self.worker_pool.submit(csv_write_task, self.folder, self.id_level_file_name, id_levels),
            self.worker_pool.submit(csv_write_task, self.folder, self.id_object_file_name, id_objects)
        ]

        wait(futures)


def stage_time_wrapper(stage: Stage, root_dto_futures):
    start = time.time()
    stage.run(root_dto_futures)
    end = time.time() - start
    print(f"Stage {stage.__class__.__name__} completed! Consumed {end} sec")
