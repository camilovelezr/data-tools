from setuptools import setup, find_packages

# with open("README.md", "r") as fh:
#     long_description = fh.read()

with open("./polus_pipelines/_pipelines/VERSION", "r") as fh:
    version = fh.read()
    with open("./polus_pipelines/_pipelines/VERSION", "w") as fw:
        fw.write(version)

package_data = ["_pipelines/VERSION"]

setup(
    name="polus_pipelines",
    version=version,
    author="Camilo Velez",
    author_email="camilo.velez@axleinfo.com",
    description="API for Polus Pipelines.",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    packages=find_packages(),
    package_data={"polus": package_data},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "polus-data>=0.1.0",
        "polus-plugins>=0.1.0",
    ],
)
