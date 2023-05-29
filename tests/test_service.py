import csv
import random
import unittest
from src import settings, model, service, archivers, converters
import shutil
import os


class TestCSVGeneratorService(unittest.TestCase):
    def setUp(self) -> None:
        self.zip_folder = 'test_zip'
        self.output_folder = 'test_output'
        self.archive_count = 5
        self.per_archive_file_count = 10
        settings.init_settings(
            zip_folder=self.zip_folder,
            output_folder=self.output_folder,
            archive_count=self.archive_count,
            per_archive_file_count=self.per_archive_file_count
        )

        self.dto_count = 0
        self.objects_per_root_dto = 10

        def predictable_root_dto_generator() -> model.RootDTO:
            self.dto_count += 1
            root_id = f'test-{self.dto_count}'
            return model.RootDTO(
                id=root_id,
                level=random.randint(1, 100),
                objects=[model.ObjectDTO(name=f'{root_id}-{i}') for i in range(self.objects_per_root_dto)]
            )

        self.predictable_root_dto_generator = predictable_root_dto_generator

    def tearDown(self) -> None:
        if os.path.isdir(self.zip_folder):
            shutil.rmtree(self.zip_folder)
        if os.path.isdir(self.output_folder):
            shutil.rmtree(self.output_folder)

    def test_archives_are_correct(self):
        service.csv_generator_service(random_root_dto_func=self.predictable_root_dto_generator)
        list_dir = [file for file in os.listdir(self.zip_folder) if '.zip' in file]
        self.assertEqual(len(list_dir), self.archive_count)

        root_dto_list = []

        for zip_archive_name in list_dir:
            files_in_mem = archivers.ZipArchiver().mem_unzip(os.path.join(self.zip_folder, zip_archive_name))
            self.assertEqual(len(files_in_mem), self.per_archive_file_count,
                             msg=f'проверяем, что в каждом архиве заданное количество файлов')
            for mem_file in files_in_mem:
                root_dto = converters.XMLConverter().loads(mem_file)
                root_dto_list.append(root_dto)
                self.assertGreaterEqual(root_dto.level, 1, msg='levcl должен быть в пределах от 1 до 100')
                self.assertLessEqual(root_dto.level, 100, msg='levcl должен быть в пределах от 1 до 100')
                self.assertEqual(len(root_dto.objects), self.objects_per_root_dto)

        self.assertEqual(
            len(set(root_dto.id for root_dto in root_dto_list)), self.archive_count * self.per_archive_file_count,
            msg='все ли id в rootDTO уникальны')

    def test_output_csv_with_default_gen(self):
        """
        use random_root_dto_func by default for service
        """
        service.csv_generator_service()
        list_dir = [file for file in os.listdir(self.output_folder) if '.csv' in file]
        self.assertEqual(len(list_dir), 2, msg='Ожидаем два csv файла на выходе')
        self.assertIn(settings.ID_LEVEL_CSV_NAME, list_dir)
        self.assertIn(settings.ID_OBJECT_NAME_CSV_NAME, list_dir)

        root_ids = set()
        with open(os.path.join(self.output_folder, settings.ID_LEVEL_CSV_NAME)) as f:
            for raw in csv.reader(f):
                root_id, root_level = raw[0], int(raw[1])
                self.assertGreaterEqual(root_level, 1, msg='levcl должен быть в пределах от 1 до 100')
                self.assertLessEqual(root_level, 100, msg='levcl должен быть в пределах от 1 до 100')
                root_ids.add(root_id)
        self.assertEqual(len(root_ids), self.archive_count * self.per_archive_file_count, msg="все id уникальны")

        root_ids_objects_map = {}
        with open(os.path.join(self.output_folder, settings.ID_OBJECT_NAME_CSV_NAME)) as f:
            for raw in csv.reader(f):
                root_id, object_name = raw[0], raw[1]
                if root_id not in root_ids_objects_map:
                    root_ids_objects_map[root_id] = [object_name]
                else:
                    root_ids_objects_map[root_id].append(object_name)
        self.assertEqual(len(root_ids_objects_map), self.archive_count * self.per_archive_file_count,
                         msg="все id уникальны")

        for root_id in root_ids_objects_map:
            object_names = root_ids_objects_map[root_id]
            self.assertGreaterEqual(len(object_names), 1)
            self.assertLessEqual(len(object_names), 10)
            self.assertEqual(len(set(object_names)), len(object_names))
