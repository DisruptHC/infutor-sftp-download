import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="infutor-standard-industries",
    version="1.0.0",
    author="Chakshu Tandon",
    author_email="chakshu.tandon@standardindustries.com",
    description="Infutor downloads SFTP data into a GCS bucket",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    install_requires=[
        'Click',
        'google-cloud-storage',
        'google-cloud-secret-manager',
        'paramiko'
    ],
    entry_points='''
        [console_scripts]
        infutor=app.main:main
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.6',
)
