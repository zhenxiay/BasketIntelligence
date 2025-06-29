from setuptools import setup, find_packages

setup(
    name='BasketIntelligence',
    version='0.1.8',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'pyspark',
        'google',
        'google-cloud',
        'google-cloud-bigquery',
        'sqlalchemy',
        'sqlite3',
        'psycopg2',
        "scikit-learn",
        'numpy'
    ],
    author="zhenxiay",
    author_email="yu.zhenxiao.yz@gmail.com",
    url="https://github.com/zhenxiay/BaseketIntelligence"
)
