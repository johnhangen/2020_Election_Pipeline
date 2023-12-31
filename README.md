# Election 2020 Pipeline

<p>
<img alt="Pandas" src="https://img.shields.io/badge/-Pandas-5849BE?style=flat-square&logo=pandas&logoColor=white" />
<img alt="Docker" src="https://img.shields.io/badge/-Docker-46a2f1?style=flat-square&logo=docker&logoColor=white" />
<img alt="python" src="https://img.shields.io/badge/-Python-13aa52?style=flat-square&logo=python&logoColor=white" />
<img alt="mysql" src="https://img.shields.io/badge/-mysql-F7B93E?style=flat-square&logo=mysql&logoColor=black" />
<img alt="AWS" src="https://img.shields.io/badge/-AWS-DD0031?style=flat-square&logo=amazonaws&logoColor=white" />
</p>

Welcome to the 2020 Election Data Pipeline project. This project is designed to streamline the process of collecting, cleaning, joining, and storing election-related data for the 2020 U.S. elections. Leveraging the power of Amazon RDS for robust data storage, the pipeline includes a suite of scripts for web scraping data from various APIs and handling CSV files. Our goal is to create an efficient and automated pipeline that simplifies the task of managing large volumes of election data.

## Project Overview

The 2020 Election Data Pipeline consists of several key components working together to process and store election data:

- **Amazon RDS Integration**: Utilizes Amazon Relational Database Service (RDS) as the backbone for data storage, providing secure and scalable database services.

- **Data Collection**: Implements scripts to automatically scrape election data from  APIs. Features the capability to process and push CSV data, such as detailed datasets from the US Census, directly into the Amazon RDS. These scripts are tailored to capture a comprehensive set of data points relevant to the 2020 elections.

- **Data Processing**: Involves a series of Python scripts that handle the cleaning and joining of data. These scripts ensure that the data is accurate, consistent, and ready for analysis.

- **Data Retrieval and Final Dataset Creation**: Features a set of tools to pull down data from Amazon RDS, perform final cleaning and joining operations, and create a consolidated dataset that represents a complete view of the 2020 election data.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Installing

A step-by-step series of examples that tell you how to get a development environment running. For instance:

1. Clone the repository

```bash
  git clone https://github.com/johnhangen/2020_Election_Pipeline
```

2. navigate to the project directory

```bash
  cd election2020
```

3. Create a virtual environment

```bash
  python3 -m venv venv
```

4. Activate the virtual environment

```bash
  source venv/bin/activate
```

5. Install the requirements

```bash
  pip install -r requirements.txt
```

6. Run the project

```bash
  python3 main.py
```

## File Structure

- `.env`: Environment variables file.
- `.gitignore`: Specifies intentionally untracked files to ignore.
- `LICENSE`: The license file.
- `README.md`: The README file for the project.
- `temp.py`: Temporary script file.

- `Collect/`: Collection scripts for various types of data.
  - `collect_data.py`: General data collection script.
  - `econ_data.py`: Economic data collection script.
  - `election_data.py`: Election data collection script.
  - `fips_data.py`: FIPS data collection script.
  - `__init__.py`: Marks the directory as a Python package.

- `data/`: Data files used or generated by the project.
  - `edu_att_test.csv`: Education attainment test data.
  - `FIPS.csv`: FIPS code data.

- `database_conn/`: Database connection module.
  - `db_conn.py`: Database connection script.
  - `__init__.py`: Marks the directory as a Python package.

- `logging/`: Logging related files.
  - `pol_pipeline.log`: Log file for the project.

- `src/`: Source code of the main application.
  - `main.py`: Main application script.
  - `__init__.py`: Marks the directory as a Python package.

- `Transform/`: Data transformation scripts.
  - `econ_transform.py`: Economic data transformation script.
  - `election_transform.py`: Election data transformation script.
  - `fip_transform.py`: FIP data transformation script.
  - `join_data.py`: Script for joining different datasets.
  - `__init__.py`: Marks the directory as a Python package.
  - `SQL_code/`: SQL scripts for data transformation.
    - `econ.sql`: SQL script for economic data.
    - `election_pull.sql`: SQL script for pulling election data.
    - `fips.sql`: SQL script for FIPS data.

## Future Work

- Data Visualization Website
- Supervised Learning Model to predict election results
- More data sources
- Further integration with AWS
- Dockerization
- More robust logging
