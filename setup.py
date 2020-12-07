from setuptools import setup


setup(
    name="flapjack",
    version="0.1.0",
    packages=["flapjack"],
    license="MIT",
    long_description="flapjack provides a collection of common PySpark flattening transformations.",
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)
