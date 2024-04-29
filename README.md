

## Summary

This project is modeled in the context of a music startup called Sparkify. My job is to build an ETL pipeline that extracts data from Udacity's S3, and loads it to my staging tables in Redshift for consolidation. From there, the data is copied appropriately to a Star Schema Dimensional Redshift tables, to be used for further purposes. 

* **s3://udacity-dend/song_data**: artists and songs data


* **s3://udacity-dend/log_data**: user, service and event data 


---


### AWS Redshift set-up

I set up my AWS Redshift Cluster with the specifications below:

* Cluster: 4x dc2.large nodes
* Location: US-West-2 (as Project-3's AWS S3 bucket)

### Staging tables

* **staging_events**: acts as a point of data consolidation for user data within the data pipeline, since the data is sourced from multiple sources (json files in s3)
* **staging_songs**: acts as a point of data consolidation for song data within the data pipeline, since the data is sourced from multiple sources (json files in s3)

### Fact Table

* **songplays**: Each row is representative of an instance of the service being used by a user

### Dimension Tables

* **users**: user info 
* **songs**: song info 
* **artists**: artist info 
* **time**: time regarding user song plays 

---

## Instructions

**Project has two scripts:**

* **create_tables.py**: This script drops existing tables and creates new tables.
* **etl.py**: This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into DB.


### Run create_tables.py

Type to command line:

`python3 create_tables.py`

### Run etl.py

Type to command line:

`python3 etl.py`

