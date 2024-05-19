# Steps to Install Graphframes
In order to insall `graphframes` package for pyspark network analysis, the first step is to download the `whl` file for `graphframes` package locally:

```bash
pip download graphframes
```

Then, using `scp` command to move the `whl` file to `/network` folder on midway:
```bash
scp graphframes-*.whl tianyuec@midway3.rcc.uchicago.edu:MACS_30123/final-project-les-deux-mousquetaires/network
```

Then, after setting `/network` folder as the working directory, load python module and install the package:
```bash
module load python
pip install --user graphframes-*.whl
```