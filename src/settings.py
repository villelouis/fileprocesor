ZIP_FOLDER: str = 'temp'
ARCHIVE_COUNT: int = 50
PER_ARCHIVE_FILE_COUNT: int = 100
OUTPUT_FOLDER: str = 'output'

# files with extensions:
ID_LEVEL_CSV_NAME: str = 'id_level.csv'
ID_OBJECT_NAME_CSV_NAME: str = 'id_object_name.csv'


def init_settings(**kwargs):
    """
    на случай, если настройки надо будет получить из др. источника, напр. сети
    :return:
    """
    for settings_key in kwargs:
        globals()[settings_key.upper()] = kwargs[settings_key]
