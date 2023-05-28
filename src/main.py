import service
from settings import init_settings

if __name__ == '__main__':
    init_settings()
    service.start_pipeline()
