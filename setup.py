from setuptools import setup, find_packages

setup(
    name="dab_project",
    version="0.0.3",
    description="Databricks project for Citibike ETL pipeline",
    author="Ricardo Gualter",
    packages=find_packages(where="./src"),
    package_dir={"":"./src"},
    install_requires=[
        "setuptools"
    ],
    entry_points={
        "packages": [
            "main=dab_project.main:main"
        ]
    }
)