from setuptools import setup, find_packages

setup(
    name='BasketIntelligence',
    version='1.0.1',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'google',
        'google-cloud',
        'google-cloud-bigquery',
        'sqlalchemy',
        'psycopg2',
        "scikit-learn",
        'numpy'
    ],
    extras_require={
        'spark': ['pyspark==3.5.1'],
    },
    author="zhenxiay",
    author_email="yu.zhenxiao.yz@gmail.com",
    url="https://github.com/zhenxiay/BaseketIntelligence"
)
