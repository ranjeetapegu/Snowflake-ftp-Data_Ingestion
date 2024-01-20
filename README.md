# Snowflake-ftp-Data_Ingestion
This project explores the different ways to consume ftp data into snowflake environment.

Pre-requisite 
Snowflake Access: A valid user account with proper privileges on your Snowflake platform.
Ananconda installed in your machine.
FTP Server:Access to an FTP server hosting the files, Make sure your credentials are ready for a smooth connection (if required). For this project I am using

## Requirement
This Project requires python 3.9 to run.Create a virtual environment.   
snowflake-snowpark-python


## using virtualenv


Create virtualenv with python 3 as default   
```conda create --name snfenv python=3.9 numpy pandas pyarrow```

Activate virtualenv   
```conda activate snfenv```

Install requirements   
```pip install snowflake-snowpark-python```

