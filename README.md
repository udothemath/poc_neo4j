# go for neo4j
## off-line
1. Create virtual environment
	- ```$ python -m venv <name_of_venv_with_path> ```
2. Activate virtual environment
	- ```$ source <name_of_venv_with_path>/bin/activate ```
3. Install python package
	- ```$ pip install -r requirements_offline.txt ```
4. Initialize neo4j (graph database). Add $ to run at background.
	- ```$ neo4j start &```
4. Open browser with port forwarding
	- ```http://localhost:7474```
5. Execute by cypher code

## aicloud
###  Connect neo4j
0. Ensure that you pick the correct image (esun_graph)
1. Reset the neo4j setting
	- replace neo4j.conf file
	```bash 
	$ source reset_neo4j_config.sh
	```
	- Note: include the following setting in neo4j.conf
		- comment out import setting
			```bash=
			#dbms.directories.import=/var/lib/neo4j/import
			```
		- uncomment following line to import by desired
			```bash
			dbms.security.allow_csv_import_from_file_urls=true
			```
		- disable authentication (no need for user and password)
			```bash
			dbms.security.auth_enabled=false
			```
2. Connect neo4j (Run at background)
	- run neo4j console at background
	```bash
	$ neo4j console &
	```	
3. There are three ways to communicate with neo4j
	- shell: 
		```bash 
		$ cypher-shell 
		```
	- python script:
		```bash
		$ python main.py
		```
	- through browser with port forward 
		1. Type ```bash localhost:7474``` on browser
	        2. Username/Password (default setting): neo4j/neo4j
			![image](https://user-images.githubusercontent.com/10674490/226319017-339f3623-d3b2-4b15-b51e-6571c838d320.png)
		3. Port forward on vscode
		    - ![image](https://user-images.githubusercontent.com/10674490/226319418-44d8fed4-504c-4ebc-81be-862ace0c4adb.png)
		    - ![image](https://user-images.githubusercontent.com/10674490/226319609-63b1b70c-293e-42cf-9844-eafab1ee9fce.png)

### Check the neo4j config setting
- default import path
	> Call dbms.listConfig() YIELD name, value
				WHERE name='dbms.directories.import'
				RETURN name, value;

	#### Note: Default: /var/lib/neo4j/import
- customized import directory
	> Call dbms.listConfig() YIELD name, value
				WHERE name='dbms.security.allow_csv_import_from_file_urls'
				RETURN name, value;
	#### Note: Default: true
- Note: Ensure comment out "dbms.directories.import" setting in neo4j.conf to import csv data from preferred directory

### Load csv file 
- from import directory
	> load csv with headers from 'file:///artists.csv' as row return count(row);
- from preferred directory
	> load csv with headers from 'file:////home/jovyan/ml_with_graph_algorithms/nice_graph/neo4j_go/artists_with_header.csv' as row return count(row);
 
 
