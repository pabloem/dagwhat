#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


"""Setup file for the Dagcheck library."""

import os
from setuptools import setup, find_packages  # type: ignore

BUILD_ID = os.environ.get("BUILD_BUILDID", "0")

setup(
    name="dagcheck",
    author="Pablo E.",
    author_email="pabloem@apache.org",
    description="A local testing framework for Airflow DAGs.",
    long_description="Dagcheck is a framework to assert for DAG invariants. "
    "Users of dagcheck can define DAG invariants to test via DAG "
    "assertions, and dagcheck will generate DAG run scenarios that "
    "verify these invariants.",
    version="0.1" + "." + BUILD_ID,
    packages=find_packages(),
    install_requires=["apache-airflow"],
    keywords=["airflow", "testing", "apache airflow", "apache"],
    extras_require={
        "test": ["pytest", "pytest-nunit", "pytest-cov", "parameterized"]
    },
    package_url="https://github.com/bitybyte/dagcheck",
    package_download_url="https://pypi.python.org/pypi/dagcheck",
    classifiers=[
        "Intended Audience :: End Users/Desktop",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Testing",
    ],
)
