# FLEXCoop Middleware

This is the middleware project to manage OpenADR communication and other functionalities for the FLEXCoop project

## Installation

1. Create a virtualenv using python 3 and install the required packages
    ```bash
    virtualenv -p python3 venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
2. Create a file with the environment variables required (config.sh is already in .gitignore to avoid uploading it to the repo):

    ```bash
    export HOST=''
    export PORT=''
    export VTN_PREFIX=''
    export VEN_PREFIX=''
    export MONGO_URI='mongodb://<user>:<password>@<mongo_host>:<mongo_port>/<db>'
    ```
3. Load the environment variables and run the server
    ```bash
    source config.sh
    python app.py
    ``` 
    
## Project Structure

The project has the following software components:

    - oadr_core
    - visual_interface
    - mongo_orm.py
    - app.py
    - settings.py

### oadr core
This package contains the openADR implementation and a blueprint for the SimpleHttp server. It is implemented in this way so we can reuse the oadr XML response generation for the XMTP server.

    - oadr_payloads: Contains functions to generate each oadr payload needed for the communication.
    - oadr_xml_example: Some xml examples of oadr payloads.
    - oadr_xml_schema: Contains the XML schema files (XSD) required to validate the XML payloads.
    - ven: Implementation of the VEN payload's management
    - vtn: Implementation of a VTN payload's management
    - oadr_base_service: Main class to deal with oadr payloads.
    - server_blueprint: Contains the SimpleHTTP server routes

### oadr visual interface

    - visual interface to manage OpenADR communication(list ven's, register to reports, etc)
    
### mongo_orm.py
    - Main class to manage a mongo Database and map the resources to python objects
### app.py
    - Main HTTPSimple server, will serve all HTTP resources, can be updated with new blueprints.
### settings.py
    - Settings of the Server application
    
 
