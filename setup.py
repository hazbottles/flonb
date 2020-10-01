import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="flonb",
    version="0.0.1",
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
    python_requires='>=3.6',
)