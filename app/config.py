from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: SecretStr = SecretStr("dev-api-key-change-me")
    database_url: str = "postgresql+asyncpg://payments:payments-local@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments-local@localhost:5672/"
    outbox_poll_interval: float = 0.5
    webhook_timeout: float = 5.0
    gateway_delay_min: float = 2.0
    gateway_delay_max: float = 5.0
    gateway_success_rate: float = 0.9

    @model_validator(mode="after")
    def validate_settings(self):
        if not self.api_key.get_secret_value().strip():
            raise ValueError("API_KEY не должен быть пустым")
        if not 0 <= self.gateway_delay_min <= self.gateway_delay_max:
            raise ValueError("Некорректный диапазон задержки шлюза")
        if not 0 <= self.gateway_success_rate <= 1:
            raise ValueError("Вероятность успеха должна быть от 0 до 1")
        if self.outbox_poll_interval <= 0 or self.webhook_timeout <= 0:
            raise ValueError("Интервалы должны быть положительными")
        return self
