from flask import Flask, jsonify
from dataRetriever import DataRetriever
from json import dump
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
db_retriever = DataRetriever()

@app.route ('/api/getDriverStandings/<driverId>', methods = ['GET'])
def getRaces (driverId):
    data = db_retriever.getDriverStandings(driverId)
    return jsonify(data)

@app.route ('/api/getHead2Head/<raceId>/<driver1>/<driver2>', methods = ['GET'])
def gethead2head (raceId, driver1, driver2):
    data = db_retriever.getHead2Head(raceId, driver1, driver2)
    return jsonify(data)

@app.route ('/api/getConstructorStandings/<constructorId>', methods = ['GET'])
def getConstructorStandings (constructorId):
    data = db_retriever.getConstructorStandings(constructorId)
    return jsonify(data)

@app.route ('/api/getRaceResults/<raceId>', methods = ['GET'])
def getRaceResults (raceId):
    data = db_retriever.getRaceResults(raceId)
    return jsonify(data)

@app.route ('/api/getRaceTimes/<raceId>/<driverId>', methods = ['GET'])
def getRaceTimes (raceId, driverId):
    data = db_retriever.getRaceTimes(raceId, driverId)
    return jsonify(data)

@app.route ('/api/getQualyData/<raceId>/<driverId>', methods = ['GET'])
def getQualyData (raceId, driverId):
    data = db_retriever.getQualyData(raceId, driverId)
    return jsonify(data)

@app.route ('/api/getRacesYears/<driverId>/<year1>/<year2>', methods = ['GET'])
def getRacesYears (driverId, year1, year2):
    data = db_retriever.getRacesYears(driverId, year1, year2)
    return jsonify(data)



if __name__ == "__main__":
    app.run(debug=True, port=8080)
