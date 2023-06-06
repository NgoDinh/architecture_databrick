- Objective: create a datawarehouse and data flow using spark.
- Work flow:
    - Step 1: Create azure account and databrick service
    - Step 2: Create cluster in databrick
    - Step 3: Create data storage (data lake) in azure with 3 container: brozen, silver and gold
    - Step 4: Create key vault to manage access key
    - Step 5: Mount data from storage to databrick
    - Step 6: Upload data from local to data lake
    - Step 7: Ingest from bronze to silver
    - Step 8: Transform from silver to gold
    - Step 9: Organize workflow
- Folder organization
    - Setup: all config need to run to connect and mount data
    - Executor: all script using to run a child job. For example: To ingest formula1_schema we need to ingest a lot of table with different type (folder, csv, json), all script to ingest them will be storage in "ingest_formula1_schema" in executor folder
    - Driver: where to run the main job (included all sub job in folder with the same name in executor folder).
    - Analysis: After allthing ready, this folder save the code to analyse data in gold schema.
    - Demo: some practice not included above workflow.