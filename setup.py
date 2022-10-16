from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="jaffle",
        packages=find_packages(exclude=["jaffle_tests"]),
        install_requires=[
            "dagster",
            "pandas",
            "duckdb",
        ],
        extras_require={
            "dev": ["dagit", "pytest", "localstack", "awscli", "awscli-local"]
        },
    )
