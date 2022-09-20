import os
from setuptools import setup, find_packages

BUILD_ID = os.environ.get("BUILD_BUILDID", "0")

setup(
    name="dagwhat",
    version="0.1" + "." + BUILD_ID,
    # Author details
    packages=find_packages("dagwhat", exclude=['test']),
    #package_dir={"": "src"},
    setup_requires=["apache-airflow"],
    tests_require=["pytest", "pytest-nunit", "pytest-cov"],
)
