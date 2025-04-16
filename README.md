Tap-Excel
Tap-Excel is a Singer tap for extracting data from Excel files, including incremental sync functionality. This tap can handle multiple sheets within the same Excel file and allows users to specify a replication_key for incremental syncing.

Features
Extract Data from Excel: Supports extracting data from multiple sheets within an Excel file.

Incremental Sync: Can perform incremental sync based on a specified replication_key.

Flexible Config File: The configuration file allows you to define which sheets to load, whether to perform incremental syncing, and how to handle missing columns for replication.

Requirements
Python 3.7+

Pandas for handling Excel data

xlsx2csv for reading .xlsx files

Installation
To install the dependencies for this tap, you can use pip:

bash
Copy
Edit
pip install -r requirements.txt
Configuration File (config.json)
The configuration file defines the Excel file to be used, the sheets to be loaded, and whether incremental syncing should be performed.

Configuration Structure
json
Copy
Edit
{
  "file_path": "Book1.xlsx",
  "sheets": [
    {
      "name": "Sheet1",                // Name of the sheet to be loaded
      "replication_key": "OrderID"     // Optional: The column used for incremental sync
    },
    {
      "name": "Sheet2",
      "replication_key": "CustomerID" // Optional: The column used for incremental sync
    }
  ]
}
Key Configuration Options
file_path: The path to the Excel file that contains the sheets to be loaded. This is a required field.

sheets: An array of sheet configurations.

name: The name of the sheet in the Excel file. This is required.

replication_key (Optional): The column used for incremental sync. If you don’t want incremental syncing, you can omit this field.

Example Configurations
Example 1: Incremental Sync for Multiple Sheets
json
Copy
Edit
{
  "file_path": "Book1.xlsx",
  "sheets": [
    {
      "name": "Sheet1",
      "replication_key": "OrderID"
    },
    {
      "name": "Sheet2",
      "replication_key": "CustomerID"
    }
  ]
}
Example 2: Full Sync for Multiple Sheets (No Replication Key)
json
Copy
Edit
{
  "file_path": "Book1.xlsx",
  "sheets": [
    {
      "name": "Sheet1"
    },
    {
      "name": "Sheet2"
    }
  ]
}
Example 3: Load All Sheets (No Sheets Defined)
json
Copy
Edit
{
  "file_path": "Book1.xlsx"
}
In this case, all sheets from the Excel file will be loaded, and if any sheets have a replication_key defined, incremental syncing will be performed.

Usage
Run the Tap
To run the tap, use the following command:

bash
Copy
Edit
tap-excel --config config.json --discover
This will initiate the discovery process, where the tap will read the sheets from the Excel file and display the available streams.

To begin the actual sync, run:

bash
Copy
Edit
tap-excel --config config.json --sync
This will start the extraction of data based on the configuration and sync the data to the target destination.

Error Handling
Missing replication_key: If a replication_key is specified but the corresponding column is missing from the sheet, the sync will fail with an error message.

No replication_key: If no replication_key is specified, a full sync will be performed for the sheet.

Logging
Logging information will be printed to the console during sync operations. This includes information about the number of records processed, errors encountered, and sync progress.

Notes
If the replication_key is defined, the sync will be incremental. If the column specified by replication_key is missing, the sync will fail.

If no replication_key is defined, a full sync will be performed.

Sheets that are not listed under "sheets" will be ignored.

Missing sheets will not cause the sync to fail but will result in no data being extracted from those sheets.

Contributing
Feel free to open issues or pull requests to contribute to this project.