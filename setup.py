from setuptools import setup, find_namespace_packages

setup(
    name="hstocks",
    description="Fetches and writes stock prices to hledger format",
    author="kkorolyov",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_namespace_packages("src"),
    install_requires=("pyyaml", "yfinance"),
    entry_points={"console_scripts": ("hstocks=hstocks.__main__:main")},
)
