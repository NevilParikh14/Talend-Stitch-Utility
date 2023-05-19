# Report_generation

* Provide the inputs as shown below
* sudo apt-get install whiptail
* sudo apt-get install -y boxes
* pip install -r requirements.txt
* run main.py file

Example 1 : If only needed to be synced in 1 branch 
```
[master_updated]
python_version = 3.9(In which venv Tap is needed to be executed)
file_already_present = yes|no
property = -p|--catalog
tap_name = chargebee
path = /opt/chargebee
folder_name = master_1
config_tap = {
    "start_date": "",
    "api_key": "",
    "site": ""
    }
bookmark_tap =
```
**Note:** Keep indentation same in "config_tap" if not provided in single line.

Example 2 : Sync and compare 2 branches same tap
```
[branch_1]
python_version = 3.9(In which venv Tap is needed to be executed)
file_already_present = yes|no
property = -p|--catalog
tap_name = chargebee
path = /opt/chargebee
folder_name = master_1
config_tap = {
    "start_date": "",
    "api_key": "",
    "site": ""
    }
bookmark_tap =

[branch_2]
python_version = 3.9(In which venv Tap is needed to be executed)
file_already_present = yes|no
property = -p|--catalog
tap_name = chargebee
path = /opt/chargebee
folder_name = master_2
config_tap = {
    "start_date": "",
    "api_key": "",
    "site": ""
    }
bookmark_tap =
```
Multiple branches can be synced if they are of diff Tap's. But for comparision only 2 branche's of same tap 
