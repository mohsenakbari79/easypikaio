from setuptools import setup, find_packages
from pathlib import Path


this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

VERSION = '0.0.3'
DESCRIPTION = 'Library for easy connection to rabbitmq  with pika Library'

# Setting up
setup(
    name="easypikaio",
    version=VERSION,
    author="mohsen akbari (malevin)",
    author_email="malevin.git@gmail.com",
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=['pika'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['python', 'pika', 'amqp', 'rabbitmq', 'iot', 'easy'],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
