from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    AMQP_URL: str = "amqp://guest:guest@localhost:5672/"
    REDIS_URL: str = "redis://localhost:6379/0"
    ANTHROPIC_API_KEY: str
    PORT: int = 8002

    class Config:
        env_file = ".env"


settings = Settings()
