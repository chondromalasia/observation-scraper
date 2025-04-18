from setuptools import setup, find_packages

def read_requirements(filename: str):
    with open(filename) as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="observation_scraper",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=read_requirements('requirements.txt'),
    python_requires=">=3.11.4",
)
