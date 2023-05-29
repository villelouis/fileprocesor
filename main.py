from src import service
from src.settings import init_settings

if __name__ == '__main__':
    init_settings()
    service.csv_generator_service()
