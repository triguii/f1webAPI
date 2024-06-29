# F1Web API

This repository holds the source code for the API that is used in order to connect the frontend of the webpage with the MongoDB database.

## File distribution

Inside the api folder we can find two files with the python code:

- index.py : this holds the code necessary to deploy the flask server, as well as the code with all the different endpoints of the API.

- dataRetriever.py : this file contains the class that connects to the MongoDB database and performs the necessary queries in order to gather the data.

## Getting Started

To start the server locally, just execute the server.py file

```bash

python .\api\server.py

```

The API will start executing on http://localhost:8080 . To check if it's working properly, access http://localhost:8080/api/getRaceResults/1 and make sure that it returns a .json file.


