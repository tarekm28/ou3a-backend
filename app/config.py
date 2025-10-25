import os

API_KEY = os.getenv("API_KEY", "CHANGE_ME_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL")
BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
MAX_BODY_MB = int(os.getenv("MAX_BODY_MB", "20"))
