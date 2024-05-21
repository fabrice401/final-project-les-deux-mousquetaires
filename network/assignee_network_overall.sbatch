#!/bin/bash

#SBATCH --job-name=assignee_network_overall
#SBATCH --output=assignee_network_overall.out
#SBATCH --error=assignee_network_overall.err
#SBATCH --ntasks=10
#SBATCH --partition=caslake
#SBATCH --account=macs30123

module load python spark

export PYSPARK_DRIVER_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3

# Specify the path to graphframes jar file
GRAPHFRAMES_JAR=graphframes-0.8.2-spark3.2-s_2.12.jar

# Include the --jars option in the spark-submit command
spark-submit --master local[*] --jars $GRAPHFRAMES_JAR assignee_network_overall.py

# Filter out INFO, WARN, and rdd messages from the output file
sed -i -r -e '/INFO|WARN/d' -e '/rdd_[0-9]+_[0-9]+/d' assignee_network_overall.out