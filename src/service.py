import os.path
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, ThreadPoolExecutor
from typing import List

from src import converters, utils, model, archivers
import csv


def generate_and_zip_files(
        process_pool_executor: ProcessPoolExecutor,
        folder_path: str = './temp',
        archive_count: int = 50,
        archive_file_count: int = 100,
        converter: converters.Converter = converters.XMLConverter(),
        archiver: archivers.Archiver = archivers.ZipArchiver()
):
    os.makedirs(folder_path, exist_ok=True)

    chunks_to_archive = [[]]
    futures = []

    for _ in range(archive_count * archive_file_count):
        futures.append(process_pool_executor.submit(converter.dumps, utils.get_random_object_dto()))

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
        futures.append(process_pool_executor.submit(archiver.zip, os.path.join(folder_path, f'{i + 1}'), chunk))
    wait(futures)


def append_csv_line(file, lock: threading.Lock, data: List[str]):
    with lock:
        csv.writer(file).writerow(data)


def unzip_and_write_to_files(
        process_pool_executor: ProcessPoolExecutor,
        input_folder: str = './temp',
        output_folder: str = './results',
        input_converter: converters.Converter = converters.XMLConverter(),
        archiver: archivers.Archiver = archivers.ZipArchiver()
):
    os.makedirs(output_folder, exist_ok=True)

    futures = []
    for zip_file in (file for file in os.listdir(input_folder) if file.split('.')[-1] == archiver.EXTENSION):
        futures.append(process_pool_executor.submit(archiver.mem_unzip, os.path.join(input_folder, zip_file)))

    root_dto_futures = []
    for future in as_completed(futures):
        xml_str_list = future.result()
        root_dto_futures.extend(
            (process_pool_executor.submit(input_converter.loads, xml_str) for xml_str in xml_str_list))

    id_level_file_path = os.path.join(output_folder, 'id_level.csv')
    id_object_name_file_path = os.path.join(output_folder, 'id_object_name.csv')

    if os.path.exists(id_level_file_path):
        os.remove(id_level_file_path)
    if os.path.exists(id_object_name_file_path):
        os.remove(id_object_name_file_path)

    id_level_file = open(id_level_file_path, 'a+')
    id_object_name_file = open(id_object_name_file_path, 'a+')

    id_level_file_lock = threading.Lock()
    id_object_name_file_lock = threading.Lock()

    csv_features = []
    with ThreadPoolExecutor() as thread_pool_executor:
        for future in as_completed(root_dto_futures):
            root_dto: model.RootDTO = future.result()
            csv_features.append(
                thread_pool_executor.submit(
                    append_csv_line, id_level_file, id_level_file_lock, [root_dto.id, str(root_dto.level)]))
            for object_dto in root_dto.objects:
                csv_features.append(
                    thread_pool_executor.submit(
                        append_csv_line, id_object_name_file, id_object_name_file_lock, [root_dto.id, object_dto.name]))

    id_level_file.close()
    id_object_name_file.close()


def start_pipeline(max_workers=None):
    with ProcessPoolExecutor(max_workers=max_workers) as ppe:
        generate_and_zip_files(ppe)
        unzip_and_write_to_files(ppe)


if __name__ == '__main__':
    start_pipeline()
