from setuptools import setup, find_packages

setup(
    name='DSS-Framework',
    version='1.0.0',
    packages=find_packages(),
    url='http://www.curwsl.org',
    license='',
    author='hasitha',
    author_email='hasithadkr7@gmail.com',
    description='Decision support system'
    install_requires=['mysql.connector', 'pandas', 'numpy'],
    zip_safe=True
)
