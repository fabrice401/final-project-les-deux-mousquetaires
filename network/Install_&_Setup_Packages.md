# Steps to Install and Stepup Packages for Using Sinteractive
Note: This markdown file details how to use `Graphframe` and `node2vec` when using sinteractive. For using sbatch jobs, please directly refer to the sbatch files in [`\network`](\network) folder.

## Graphframe
### Install
In order to use `graphframe` spark package for pyspark network analysis, the first step is to download the `.jar` file (via this [website](https://spark-packages.org/package/graphframes/graphframes)) for `graphframe` package locally (note: here I chose the version that matches the pyspark version in midway).

Then, move the `jar` file (downloaded ) for `graphframe` package to `/network/packages` folder on midway:
```bash
scp graphframes-0.8.2-spark3.2-s_2.12.jar tianyuec@midway3.rcc.uchicago.edu:MACS_30123/final-project-les-deux-mousquetaires/network/packages
```
### Documentation for graphframe
- [Graphframes overview](https://graphframes.github.io/graphframes/docs/_site/index.html)
- [pygraphframes documentation](https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.html)

## node2vec
### Install
In order to use lastest `node2vec` package, the first step is to download the `.whl` file (see [here](final-project-les-deux-mousquetaires/network/node2vec-0.4.6-py3-none-any.whl)) locally via this [website](https://pypi.org/project/node2vec/#files) (or you can just directly use `pip download`).

```bash
# Move the whl file to midway
scp node2vec-*.whl tianyuec@midway3.rcc.uchicago.edu: MACS_30123/final-project-les-deux-mousquetaires/network/packages
# Install the package
pip install final-project-les-deux-mousquetaires/network/packages/node2vec-*.whl
```

Then, find directory that stores `libmkl_rt.so.1` file: 
```bash
# Check directories that stores `libmkl_rt.so.1` file
find / -name "libmkl_rt.so.1" 2>/dev/null
```

## transformer
In order to use the pre-trained bert model, the first step is to download the `.whl` file of `transformer` package (via this [link](https://pypi.org/project/transformers/#files)). Then install it on midway:
```bash
# Move the whl file to midway
scp transformers-*.whl tianyuec@midway3.rcc.uchicago.edu: MACS_30123/final-project-les-deux-mousquetaires/network/packages

# Install the package
pip install final-project-les-deux-mousquetaires/network/packages/transformers-*.whl
```

The next step is to download the pre-trained BERT (uncased) model from [Hugging Face](https://huggingface.co/google-bert/bert-base-uncased). After finished cloning the repostiory that stores pre-trained BERT (uncased) model, zip the file and then trasnfer it to midway:
```bash
# Zip the folder
zip -r bert-base-uncased.zip bert-base-uncased

# Move the zip file to midway
scp bert-base-uncased.zip tianyuec@midway3.rcc.uchicago.edu: MACS_30123/final-project-les-deux-mousquetaires/network

# Unzip the file
cd network/
export UNZIP_DISABLE_ZIPBOMB_DETECTION=TRUE
unzip bert-base-uncased.zip # press 'A' to ensure that all files are extracted and are okay with overwriting any duplicates
rm bert-base-uncased.zip
cd ..
```

## Setup for sinteractive
- Initiate:
    ```bash
    # Initiate sinteractive
    sinteractive --ntasks=16 --account=macs30123

    module load python spark

    export PYSPARK_DRIVER_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3
    # Export the LD_LIBRARY_PATH to include the directory that stores `libmkl_rt.so.1` file
    export LD_LIBRARY_PATH=/home/veeratejg/anaconda3/lib:$LD_LIBRARY_PATH
    ```
- Interactive pyspark console:
    ```bash
    pyspark --jars packages/graphframes-0.8.2-spark3.2-s_2.12.jar
    ```

- Submitting a complete python script:
    ```bash
    pyspark --jars packages/graphframes-0.8.2-spark3.2-s_2.12.jar -i [specific python script]
    ```

- When finished:
    ```bash
    # Exit pyspark
    exit()

    # Exit sinteractive
    exit()
    ```