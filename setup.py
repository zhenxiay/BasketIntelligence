from setuptools import setup, find_packages

setup(
    name='BasketIntelligence',
    version='1.0.1',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'pyspark==3.5.1',
        'google',
        'google-cloud',
        'google-cloud-bigquery',
        'sqlalchemy',
        'psycopg2',
        "scikit-learn",
        'numpy'
    ],
    author="zhenxiay",
    author_email="yu.zhenxiao.yz@gmail.com",
    url="https://github.com/zhenxiay/BaseketIntelligence"
)
