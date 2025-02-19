## Quickstart
```bash
# Install Java Development Kit (JDK), required for running Apache Spark
sudo apt install default-jdk -y

# Install additional dependencies: curl (for downloading files), mlocate (for file indexing), git, and Scala (Spark is written in Scala)
sudo apt install curl mlocate git scala -y

# Download the Apache Spark binary package (version 3.2.0 with Hadoop 3.2 support)
curl -O https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz

# Extract the downloaded tarball to the current directory
sudo tar xvf spark-3.2.0-bin-hadoop3.2.tgz

# Create a dedicated directory for Spark installation
sudo mkdir /opt/spark

# Move the extracted Spark files to the /opt/spark directory
sudo mv spark-3.2.0-bin-hadoop3.2/* /opt/spark

# Grant full read, write, and execute permissions to all users for the Spark directory
# (Note: This is generally not recommended for production environments. Use more restrictive permissions if possible.)
sudo chmod -R 777 /opt/spark

# Set the SPARK_HOME environment variable to point to the Spark installation directory
export SPARK_HOME=/opt/spark

# Add Spark's bin and sbin directories to the system PATH to enable Spark commands globally
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Start the Spark master node (runs on the default port 7077)
start-master.sh

# Start a Spark worker node and connect it to the master node at spark://rnd-spark:7077
start-worker.sh spark://rnd-spark:7077
```
```bash
# Install essential tools and libraries required for compiling Python packages
sudo apt install build-essential libssl-dev libffi-dev python3-dev -y

# Install Python3 package manager (pip) and virtual environment tools
sudo apt install python3-pip python3-venv -y

# Create a new directory for the NASA log analyzer project
mkdir nasa-log-analyzer

# Change to the project directory
cd nasa-log-analyzer/

# Create a Python virtual environment in the current directory
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Upgrade pip to the latest version
pip install --upgrade pip

# Install required Python libraries:
# - jupyter: For running Jupyter notebooks
# - pyspark: The Python API for Apache Spark
# - pandas: For data manipulation and analysis
# - py4j: Required for PySpark to communicate with the JVM
pip install jupyter pyspark pandas py4j

# Install seaborn for data visualization
pip install -U seaborn

# Open a new Jupyter notebook file named nasa-log-analyzer.ipynb in the default code editor
code nasa-log-analyzer.ipynb

```


## Reference
https://opensource.com/article/19/5/log-data-apache-spark