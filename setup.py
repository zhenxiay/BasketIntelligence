from setuptools import setup, find_packages

setup(
    name='BasketIntelligence',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'pyspark'
    ],
    author="zhenxiay",
    author_email="yu.zhenxiao.yz@gmail.com",
    url="https://github.com/zhenxiay/BaseketIntelligence"
)