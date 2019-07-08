from setuptools import setup

required = ["confluent-kafka[avro]>=1.0", "pyaml>=3.13"]


setup(
    name="pyconnect",
    version="0.4.0",
    packages=["pyconnect"],
    package_dir={"": "src"},
    # minimal requirements to run pyconnect
    install_requires=required,
    url="https://github.com/real-digital/pyconnect",
    license="MIT",
    author="real.digital",
    extras_require={"test": ["pytest", "pytest-mock", "pytest-cov"], "dev": ["black", "flake8"]},
    author_email="opensource@real-digital.de",
    description='A Python implementation of "Kafka Connect"-like functionality',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
)
