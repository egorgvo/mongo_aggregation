import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="mongo-aggregation",
    version="1.0.3",
    description="Python MongoDB aggregation ORM",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/egorgvo/mongo_aggregation",
    author="Egor Gvo",
    author_email="work.egvo@ya.ru",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    packages=["mongo_aggregation"],
)
