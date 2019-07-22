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
    install_requires=['data_layer', 'pandas', 'numpy'],
    dependency_links=[
        'git+https://github.com/CUrW-SL/data_layer.git@master#egg=data_layer-0.0.1',
        'git+https://github.com/CUrW-SL/algo_wrapper.git@master#egg=algo_wrapper-1.0.0'
    ],
    zip_safe=True
)
