from setuptools import setup

setup(
    name='tap-excel',
    version='0.1',
    py_modules=['tap_excel'],
    install_requires=[
        'singer-python',
        'pandas',
        'openpyxl'
    ],
    entry_points='''
        [console_scripts]
        tap-excel=tap_excel:main
    ''',
)
