import setuptools


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="rolling_prefetch", # Replace with your own username
    version="0.0.1",
    author="Valerie Hayot",
    author_email="valeriehayot@gmail.com",
    description="A script to prefetch concurrently prefetch S3 files while proceesing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/valhayot/rollingprefetch",
    project_urls={
        "Bug Tracker": "https://github.com/valhayot/rollingprefetch/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["s3fs"],
    packages=setuptools.find_packages(),
    python_requires=">=3.8",
)
