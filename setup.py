from setuptools import setup, find_packages
from pathlib import Path

module_dir = Path(__file__).resolve().parent

with open(module_dir / "README.md") as f:
    long_description = f.read()

setup(
    name="osmthedistance",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Distance-goal routing using OpenStreetMap data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dwinston/osmthedistance",
    author="Donny Winston",
    author_email="dwinston@alum.mit.edu",
    license="AGPL",
    packages=find_packages(),
    install_requires=[
        "beautifulsoup4",
        "haversine",
        "lxml",
        "pydash",
        "pymongo",
        "requests",
        "tqdm",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires='>=3.5',
)
