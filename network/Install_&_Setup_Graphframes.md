# Steps to Install and Stepup Graphframes

## Install
In order to use `graphframes` spark package for pyspark network analysis, the first step is to download the `.jar` file (via this [website](https://spark-packages.org/package/graphframes/graphframes)) for `graphframes` package locally (note: here I chose the version that matches the pyspark version in midway).

Then, move the `jar` file (downloaded ) for `graphframes` package to `/network` folder on midway:
```bash
scp graphframes-0.8.2-spark3.2-s_2.12.jar tianyuec@midway3.rcc.uchicago.edu:MACS_30123/final-project-les-deux-mousquetaires/network
```

## Setup
To use it for sinteractive:
```bash
module load python spark

export PYSPARK_DRIVER_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3

pyspark --jars graphframes-0.8.2-spark3.2-s_2.12.jar
```