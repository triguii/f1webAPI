import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.json_util import dumps, loads 
from flask import Flask, jsonify



class DataRetriever ():
    def __init__(self):
        self._uri = "" # Clau per a connectar-se amb la base de dades (privat)
        self.client = MongoClient(self._uri, server_api = ServerApi('1'))
        self.db = self.client["F1Web"]

    def getDriverStandings(self, pilotId):
        pilots = {'20': 'Vettel', '4' : 'Alonso'}
        result = self.db['driver_standings'].aggregate([

            {
                '$match':{
                    'driverId': int(pilotId)
                }
            },
            {
                '$lookup':{
                    'from': 'races', 
                    'localField': 'raceId', 
                    'foreignField': 'raceId', 
                    'as': 'joinedResult'
                }

            },
            {'$unwind': '$joinedResult'},
            
            { 
                
                    '$project': { '_id': 0, 'driverId': 1, 'points': 1, pilots[pilotId] : '$points', 'Posicion '+ pilots[pilotId]: '$position','points' + pilotId : '$points', 'year': '$joinedResult.year', 'raceId': 1, 'round': '$joinedResult.round', 'raceName': '$joinedResult.name'}
            },
        ])

        return loads(dumps(result, default=str))
    
    def getHead2Head (self, raceId, driver1, driver2):

        result = self.db['driver_standings'].aggregate([

        {
            '$match':{
                '$and':[
                     {'raceId': int(raceId)},
                     {'$or' : [{'driverId': int(driver1)}, {'driverId': int(driver2)}]}]
            }
        },
        {
            '$lookup':{
                'from': 'races', 
                'localField': 'raceId', 
                'foreignField': 'raceId', 
                'as': 'joinedResult'
            }

        },
        {'$unwind': '$joinedResult'},
        
        { 
            '$project': { '_id': 0, 'driverId': 1, 'points': 1, 'points' : '$points', 'year': '$joinedResult.year', 'raceId': 1}
        },
        ])

        return loads(dumps(result, default=str))
    
    def getConstructorStandings(self, contructorId):
        constructors = {'6': 'Ferrari', '9' : 'Red Bull', '19': 'Jaguar', '24': 'Stewart'}
        result = self.db['constructor_standings'].aggregate([
            {
                '$match':{
                    'constructorId': int(contructorId)
                }
            },
            {
                '$lookup':{
                    'from': 'races', 
                    'localField': 'raceId', 
                    'foreignField': 'raceId', 
                    'as': 'joinedResult'
                }

            },
            {'$unwind': '$joinedResult'},
            {'$match':{
                    '$expr' : {'$lt' : ['$joinedResult.year', 2014]}
            }
            },
            {
            '$lookup':{
                'from': 'constructor_results', 
                'let' : { 'constructorId': "$constructorId", 'raceId': "$raceId" }, 
                'pipeline' : [{ '$match' : {'$expr': {'$and': [{'$eq' : ["$constructorId", "$$constructorId"]}, {'$eq' : ["$raceId", "$$raceId"]}]}}}],
                'as': 'resultsResult'
            }
            },
            {'$unwind': '$resultsResult'},
            
            { 
                
                '$project': { '_id': 0, "Puntos carrera " + constructors[contructorId] : '$resultsResult.points','contructorId': 1, 'Posición ' + constructors[contructorId] : '$position','points': 1, 'Puntos ' + constructors[contructorId] : '$points','points' + contructorId : '$points', 'year': '$joinedResult.year', 'raceId': 1, 'round': '$joinedResult.round', 'raceName': '$joinedResult.name', 'team' :  constructors[contructorId]}
            },
            ])

        return loads(dumps(result, default=str))
    

    def getRaceResults(self, raceId):
            result = self.db['results'].aggregate([
                {
                    '$match':{
                        'raceId': int(raceId)
                    }
                },
                {
                    '$lookup':{
                        'from': 'races', 
                        'localField': 'raceId', 
                        'foreignField': 'raceId',
                        'as': 'racesResult'
                    }

                },
                {'$unwind': '$racesResult'},
                {
                    '$lookup':{
                        'from': 'constructors', 
                        'localField': 'constructorId', 
                        'foreignField': 'constructorId',
                        'as': 'constructorsResult'
                    }

                },
                {'$unwind': '$constructorsResult'},
                {
                    '$lookup':{
                        'from': 'driver_standings', 
                        'let' : { 'driverId': "$driverId", 'raceId': "$raceId" }, 
                        'pipeline' : [{ '$match' : {'$expr': {'$and': [{'$eq' : ["$driverId", "$$driverId"]}, {'$eq' : ["$raceId", "$$raceId"]}]}}}],
                        'as': 'standingsResult'
                    }

                },
                {'$unwind': '$standingsResult'},
                {
                    '$lookup':{
                        'from': 'drivers', 
                        'localField': 'driverId', 
                        'foreignField': 'driverId', 
                        'as': 'driversResult'
                    }

                
                },
                {'$unwind': '$driversResult'},
                { 
                    
                    '$project': { '_id': 0,'id' : '$driverId', 'raceId': 1, 'name': '$racesResult.name', 'Piloto': '$driversResult.surname', 'Piloto Nombre': '$driversResult.forename', 'Vueltarapida' : '$fastestLapTime', 'Puntos Mundial' : '$standingsResult.points', 'Posicion Mundial': '$standingsResult.position'
                                 , 'Posicion Carrera': '$positionOrder', "Posicion Salida": '$grid', "Puntos Carrera": '$points', 'ronda': '$racesResult.round', 'Equipo' : '$constructorsResult.name'}
                },
            ])

            return loads(dumps(result, default=str))


    def getRaceTimes(self, raceId, driverId):
        result = self.db['lap_times'].aggregate([
            {
                '$match':{
                    '$and': [{'driverId' : int(driverId)}, {'raceId' :  int(raceId)}]
                }
            },
            {
                    '$lookup':{
                        'from': 'drivers', 
                        'localField': 'driverId', 
                        'foreignField': 'driverId', 
                        'as': 'driversResult'
                    }

                
            },
            {'$unwind': '$driversResult'},
            { 
                
                '$project': { '_id': 0, 'Vuelta': '$lap', 'Tiempo ms': '$milliseconds', 'raceId' : 1, 'Tiempo' : '$time', 'driverId' : 1, 'Piloto': '$driversResult.surname', 'Posición': '$position'}
            },
        ])

        return loads(dumps(result, default=str))
    
    
    def getQualyData(self, raceId, driverId):
        result = self.db['qualifying'].aggregate([
            {
                '$match':{
                    '$and': [{'driverId' : int(driverId)}, {'raceId' :  int(raceId)}]
                }
            },
            {
                    '$lookup':{
                        'from': 'races', 
                        'localField': 'raceId', 
                        'foreignField': 'raceId',
                        'as': 'racesResult'
                    }

            },
            {'$unwind': '$racesResult'},
            { 
                
                '$project': { '_id': 0, 'Q1': '$q1', 'Q1 ms': 1, 
                             'Q2': '$q2', 'Q2 ms': 1,
                             'Q3': '$q3', 'Q3 ms': 1, 'driverId' : 1, 'Posición' : '$position'}
            },
        ])

        return loads(dumps(result, default=str))
    
    def getRacesYears (self, driverId, year1, year2):
        result = self.db['results'].aggregate([
            {
                '$match':{
                    'driverId': int(driverId)
                }
            },
            {
                '$lookup':{
                    'from': 'races', 
                    'localField': 'raceId', 
                    'foreignField': 'raceId',
                    'as': 'racesResult'
                }

            },
            {'$unwind': '$racesResult'},
            {
            '$match':{
                '$expr': {'$and': [{'$gt' : ["$racesResult.year", int(year1)]}, {'$lt' : ["$racesResult.year", int(year2)]}]}
            }
            },
            {
                    '$lookup':{
                        'from': 'drivers', 
                        'localField': 'driverId', 
                        'foreignField': 'driverId', 
                        'as': 'driversResult'
                    }

                
            },
            {'$unwind': '$driversResult'},
            {'$project': { '_id': 0, 'driverId' : 1, 'Piloto': '$driversResult.surname', 'Posición' : '$positionOrder', 'Año': '$racesResult.year', 'Puntos': '$points',
                          'Carrera' : '$racesResult.name', 'Ronda': '$racesResult.round', "VRapida" : "$fastestLapTime"
                }
            }

        ])

        return loads(dumps(result, default=str))





    






