#!/bin/bash

#SBATCH --job-name=keywords_patent
#SBATCH --output=keyword.out
#SBATCH --error=keyword.err
#SBATCH --ntasks=10
#SBATCH --partition=caslake
#SBATCH --account=macs30123

module load python spark

export PYSPARK_DRIVER_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3

spark-submit --master local[*] keywords_cluster.py

# to filter out logging messages from output, run the following:
# cat spark.out | grep -vE "INFO|WARN"

sed -i -r -e '/INFO|WARN/d' -e '/rdd_[0-9]+_[0-9]+/d' keyword.out