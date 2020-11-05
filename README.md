# About
A PySpark Boilerplate project.

# Package Versions
This is built using
1. Apache Spark 3.0.0
2. Python 3.8

# Project Structure
The project structure consists of ```src```, ```tests``` and ```config``` directories. <br> 
<br>Here the config directory is just for reference 
and in real life use cases this is to modified with the spark configs needed.
<br>The config are in Yaml format.
<br><br>
In  source directory ```src```, there are different modules.
<br>At root of ```src``` is main.py file which is the main driver for Spark job.
<br> ```lib``` stores general purpose libraries like logger configuration class in this case.
<br>```jobs``` module stores different packages required for the data processing.
For our case we have only one package called ```recipe``` which suffices the requirements.
the py file in recipe contains all the code and custom functions needed for use case.
<br><br>
the ```tests``` section tries to replicate the same structure as of src consisting unit test cases for each functions
<br><br>
```Makefile``` consists of make scripts to create a deployable dist package.
and ```requirements.txt``` contains the list of all pip packages needed.

# Deployment
 To make a distributable directory and zip, run
```python
make build  
```
at project root directory
 <br>
 This will generate a ```main.py``` file and ```jobs.zip``` in ```dist``` directory which can be shipped to cluster
 
# Submitting Jobs
 Once the jobs.zip file is submitted to cluster nodes and main.py is available for driver, to run the job
```shell script
spark-submit --py-files dist/jobs.zip dist/main.py --config-file=<config file path> --input-file=<input filepath> --output-path=<outout filepath>
```

example

```shell script
spark-submit --py-files dist/jobs.zip dist/main.py --config-file=dist/configs/recipe_spark_configs.yaml 
```
<br> *for this particular use case, input file and output-path are optional

