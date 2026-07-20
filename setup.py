from setuptools import find_packages, setup

setup(
    name="clustered-pilot-executor",
    version="0.1.0",
    description="ClusteredPilotExecutor for Parsl: pilot jobs, clustering, and dynamic task submission.",
    author="Rafael Terra",
    author_email="rafaelstjf@gmail.com",
    packages=find_packages(),
    install_requires=[
        "parsl",
        "pyzmq",
        "pandas",
        "networkx",
        "matplotlib",
    ],
    entry_points={
        "console_scripts": [
            "cpe-worker=parsl.executors.clustered_pilot_executor.worker:main",
        ],
    },
    python_requires=">=3.8",
    include_package_data=True,
)
