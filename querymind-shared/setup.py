# querymind-shared/setup.py
from setuptools import setup, find_packages

setup(
    name="querymind-shared",
    version="2.0.0",
    description="Shared event bus infrastructure for QueryMind microservices",
    packages=find_packages(),
    python_requires=">=3.12",
    install_requires=[
        "pika>=1.3.2",
        "celery[rabbitmq]>=5.3.6",
        "pydantic>=2.6.0",
        "python-dotenv>=1.0.1",
    ],
)
