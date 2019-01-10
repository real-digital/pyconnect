from setuptools import setup

setup(
    name='pyconnect',
    version='0.2.0',
    packages=['pyconnect'],
    package_dir={'': 'src'},

    # minimal requirements to run pyconnect
    install_requires=[
        "confluent-kafka[avro]>=0.11.5",
        "pyaml>=3.13"
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
