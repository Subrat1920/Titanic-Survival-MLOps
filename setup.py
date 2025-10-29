from setuptools import setup,find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="Titanic-Survival-Prediciton-MLOps",
    version="0.1",
    author="Subrat",
    packages=find_packages(),
    install_requires = requirements,
)