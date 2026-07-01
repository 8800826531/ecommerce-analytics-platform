import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class RedpandaConfig:
    bootstrap_servers: str
    sasl_username: str
    sasl_password: str
    schema_registry_url: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "SCRAM-SHA-256"

    @classmethod
    def from_env(cls) -> "RedpandaConfig":
        return cls(
            bootstrap_servers=os.environ["REDPANDA_BOOTSTRAP"],
            sasl_username=os.environ["REDPANDA_USERNAME"],
            sasl_password=os.environ["REDPANDA_PASSWORD"],
            schema_registry_url=os.environ["REDPANDA_SCHEMA_REGISTRY_URL"],
        )

    def to_producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
        }