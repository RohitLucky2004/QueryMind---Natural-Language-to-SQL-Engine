from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    AMQP_URL: str = "amqp://guest:guest@localhost:5672/"
    REDIS_URL: str = "redis://localhost:6379/0"
    PORT: int = 8003

    # SQLAlchemy pool settings for user-provided databases
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 2
    DB_POOL_TIMEOUT: int = 30

    # Execution safety limits
    STATEMENT_TIMEOUT_MS: int = 10000   # 10 seconds
    MAX_RESULT_ROWS: int = 1000         # truncate result sets above this
    HISTORY_CAP: int = 100              # max history entries per session in Redis

    class Config:
        env_file = ".env"


settings = Settings()
