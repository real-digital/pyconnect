from setuptools import setup

setup(
    name='pyconnect',
    version='0.0.2',
    packages=['pyconnect'],
    install_requires=[
        "confluent-kafka==0.11.5",
        "avro-python3==1.8.2",
        "fastavro==0.21.4",
        "pytest==3.7.1",
        "requests==2.19.1"
    ],
    url='https://github.com/MrTrustworthy/pyconnect',
    license='MIT',
    author='MrTrustworthy',
    author_email='tinywritingmakesmailhardtoread@gmail.com',
    description='A Python implementation of "Kafka Connect"-like functionality',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Application Frameworks'
    ]
)
