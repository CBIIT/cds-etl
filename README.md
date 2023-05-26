# cds-etl transformation script
Code to transform CDS data.<br/>
To run the python script for transforming CDS data, the user should first place the raw CDS data file in the s3 bucket<br/>
```python3 cds-transformation.py cds_config_example.yaml```<br/>
To run the python script for transforming CDS data with the data format v1.2, the user should first place all the raw CDS data files in ./0-Raw-Data-Files/cds_raw_data_files_v1.2 folder than use the command to extrat raw data dictionary<br/>
```python3 1-Transformation-Scripts/cds-transformation_v1.2.py --config_file 2-Config-Files/cds_config_v1.2/cds_config_example_v1.2.yaml --extract_raw_data_dictionary```<br/>
After extracting the raw data dictionary, the user can use the command to transform raw data.<br/>
```python3 1-Transformation-Scripts/cds-transformation_v1.2.py --config_file 2-Config-Files/cds_config_v1.2/cds_config_example_v1.2.yaml```<br/>
If the user want to upload the transformed data to the s3 folder, the user can use the command below.<br/>
```python3 1-Transformation-Scripts/cds-transformation_v1.2.py --config_file 2-Config-Files/cds_config_v1.2/cds_config_example_v1.2.yaml --upload_s3```<br/>
To run the python script for transforming CDS data with the data format v1.3, the user should first place all the raw CDS data files in ./0-Raw-Data-Files/cds_raw_data_files_v1.3 folder than use the command<br/>
```python3 1-Transformation-Scripts/cds-transformation_v1.3.py --config_file 2-Config-Files/cds_config_v1.3/cds_config_example_v1.3.yaml --extract_raw_data_dictionary```<br/>
After extracting the raw data dictionary, the user can use the command to transform raw data.<br/>
```python3 1-Transformation-Scripts/cds-transformation_v1.3.py --config_file 2-Config-Files/cds_config_v1.3/cds_config_example_v1.3.yaml```<br/>
If the user want to download data from s3, the user can use the command to transform raw data.<br/>
```python3 1-Transformation-Scripts/cds-transformation_v1.3.py --config_file 2-Config-Files/cds_config_v1.3/cds_config_example_v1.3.yaml --download_s3```<br/>
If the user want to upload the transformed data to the s3 folder, the user can use the command below.<br/>
```python3 1-Transformation-Scripts/cds-transformation_v1.3.py --config_file 2-Config-Files/cds_config_v1.3/cds_config_example_v1.3.yaml --upload_s3```<br/>
