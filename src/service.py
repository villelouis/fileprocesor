import os.path
import time
from concurrent.futures import as_completed, wait, ThreadPoolExecutor, _base
from typing import Callable

from src import converters, utils, model, archivers, stages, settings


@utils.timed
def generate_and_zip_files(
        *,
        pool_executor: _base.Executor,
        folder_path: str = './temp',
        archive_count: int = 50,
        archive_file_count: int = 100,
        converter: converters.Converter = converters.XMLConverter(),
        archiver: archivers.Archiver = archivers.ZipArchiver(),
        random_root_dto_func: Callable[[], model.RootDTO] = utils.get_random_object_dto
):
    start = time.time()
    os.makedirs(folder_path, exist_ok=True)

    chunks_to_archive = [[]]
    futures = []

    for _ in range(archive_count * archive_file_count):
        futures.append(pool_executor.submit(converter.dumps, random_root_dto_func()))

    for i, future in enumerate(as_completed(futures)):

        path_with_data = model.FileNameWithData(
            file_name=f'{i + 1}.{converter.EXTENSION}',
            data=future.result()
        )

        last_chunk = chunks_to_archive[-1]
        if len(last_chunk) < archive_file_count:
            last_chunk.append(path_with_data)
        else:
            chunks_to_archive.append([path_with_data])

    futures = []
    for i, chunk in enumerate(chunks_to_archive):
        futures.append(pool_executor.submit(archiver.zip, os.path.join(folder_path, f'{i + 1}'), chunk))
    wait(futures)
    end = time.time() - start


@utils.timed
def unzip_and_write_to_files(
        *,
        output_stage: stages.OutputStage,
        pool_executor: _base.Executor,
        input_folder: str = './temp',
        output_folder: str = './results',
        input_converter: converters.Converter = converters.XMLConverter(),
        archiver: archivers.Archiver = archivers.ZipArchiver()
):
    os.makedirs(output_folder, exist_ok=True)

    futures = []
    for zip_file in (file for file in os.listdir(input_folder) if file.split('.')[-1] == archiver.EXTENSION):
        futures.append(pool_executor.submit(archiver.mem_unzip, os.path.join(input_folder, zip_file)))

    root_dto_futures = []
    for future in as_completed(futures):
        xml_str_list = future.result()
        root_dto_futures.extend(
            (pool_executor.submit(input_converter.loads, xml_str) for xml_str in xml_str_list))

    output_stage.run(root_dto_futures)


@utils.timed
def csv_generator_service(
        max_workers=None,
        random_root_dto_func: Callable[[], model.RootDTO] = utils.get_random_object_dto
):
    output_folder = settings.OUTPUT_FOLDER
    with ThreadPoolExecutor(max_workers=max_workers) as tpe:
        generate_and_zip_files(
            folder_path=settings.ZIP_FOLDER,
            archive_count=settings.ARCHIVE_COUNT,
            archive_file_count=settings.PER_ARCHIVE_FILE_COUNT,
            pool_executor=tpe,
            random_root_dto_func=random_root_dto_func
        )

        unzip_and_write_to_files(
            output_stage=stages.CSVWithPoolExecutorOutputStage(worker_pool=tpe, folder=output_folder),
            pool_executor=tpe,
            input_folder=settings.ZIP_FOLDER,
            output_folder=output_folder
        )


if __name__ == '__main__':
    csv_generator_service()
