from setuptools import setup, find_packages

setup(
    name="clustered-pilot-executor",
    version="0.1.0",
    description="ClusteredPilotExecutor for Parsl: supports pilot jobs, clustering, and dynamic task submission.",
    author="Rafael Terra",
    author_email="rafaelstjf@gmail.com",
    packages=find_packages(),
    install_requires=[
        "parsl>=1.2",   # adjust to your Parsl version
        "pyzmq",
        "networkx",
        "pandas",
        "matplotlib"
    ],
    entry_points={
        'console_scripts': [
            'adaptive_worker=adaptive_executor.worker:main',
        ],
    },
    python_requires='>=3.8',
    include_package_data=True,
)
