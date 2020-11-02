import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="flonb",
    version="0.1.0",  # make sure to also update in ./flonb/__init__.py
    author="hazbottles",
    description="Rapid-iteration data pipelines with automagic caching + parameter passing.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hazbottles/flonb",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["dask"],
    python_requires=">=3.6",
)
