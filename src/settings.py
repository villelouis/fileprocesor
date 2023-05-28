ZIP_FOLDER: str = 'temp'
ARCHIVE_COUNT: int = 50
PER_ARCHIVE_FILE_COUNT: int = 100
OUTPUT_FOLDER: str = 'output'


def init_settings():
    """
    на случай, если настройки надо будет получить из др. источника, напр. сети
    :return:
    """
    global ZIP_FOLDER
    ZIP_FOLDER = 'zip_folder'
