import os.path
from concurrent.futures import as_completed, ThreadPoolExecutor, Executor, ProcessPoolExecutor
from typing import Callable

from src import converters, utils, model, archivers, stages, settings


@utils.timed
def generate_and_zip_files(
        *,
        folder_path: str = './temp',
        archive_count: int = 50,
        archive_file_count: int = 100,
        converter: converters.Converter = converters.XMLConverter(),
        archiver: archivers.Archiver = archivers.ZipArchiver(),
        random_root_dto_func: Callable[[], model.RootDTO] = utils.get_random_object_dto
):
    chunks_to_archive = [[]]
    root_dto_list = []

    for _ in range(archive_count * archive_file_count):
        root_dto_list.append(converter.dumps(random_root_dto_func()))

    for i, root_dto in enumerate(root_dto_list):

        path_with_data = model.FileNameWithData(
            file_name=f'{i + 1}.{converter.EXTENSION}',
            data=root_dto
        )

        last_chunk = chunks_to_archive[-1]
        if len(last_chunk) < archive_file_count:
            last_chunk.append(path_with_data)
        else:
            chunks_to_archive.append([path_with_data])

    with ProcessPoolExecutor() as pool:
        pool.map(
            archiver.zip,
            [os.path.join(folder_path, f'{i + 1}') for i in range(len(chunks_to_archive))],
            [chunk for chunk in chunks_to_archive]
        )


@utils.timed
def unzip_and_write_to_files(
        *,
        output_stage: stages.OutputStage,
        pool_executor: Executor,
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
        max_workers=settings.MAX_WORKERS,
        random_root_dto_func: Callable[[], model.RootDTO] = utils.get_random_object_dto
):
    output_folder = settings.OUTPUT_FOLDER

    os.makedirs(settings.ZIP_FOLDER, exist_ok=True)

    generate_and_zip_files(
        folder_path=settings.ZIP_FOLDER,
        archive_count=settings.ARCHIVE_COUNT,
        archive_file_count=settings.PER_ARCHIVE_FILE_COUNT,
        random_root_dto_func=random_root_dto_func
    )

    with ThreadPoolExecutor(max_workers=max_workers) as tpe:
        unzip_and_write_to_files(
            output_stage=stages.CSVWithPoolExecutorOutputStage(worker_pool=tpe, folder=output_folder),
            pool_executor=tpe,
            input_folder=settings.ZIP_FOLDER,
            output_folder=output_folder
        )


if __name__ == '__main__':
    csv_generator_service()
