/**
 *
 * Created By Raghav on 04/09/2016
 *
 */
var express = require('express');
var redis = require('redis');
var mysql = require('mysql');
var _ = require('lodash');
var csvParser = require('csv-parse');
var fs = require('fs');
var app = express();

var toppr = require('./toppr');

var connection = mysql.createConnection({
  host: '127.0.0.1',
  user: 'root',
  password: '',
  database: 'node_db2'
});

connection.connect(function (err) {
  if (err) {
    console.error('error connecting: ' + err.stack);
    return;
  }
  console.log('connected as id ' + connection.threadId);
});

var db = {};
db.redisWrite = redis.createClient(6379, "127.0.0.1"); //redis client for write operations
db.redisRead = redis.createClient(6379, "127.0.0.1");  //redis client for read operations
db.mySql = connection; //mysql client



// toppr.getBattlesList(db, function(err, reply){
// 	console.log(err, reply);
// })

/**
 * url to get the list of battles.
 */
app.get('/list', function(req, res){
	//console.log("list")
	toppr.getBattlesList(db, function(err, reply){
		//console.log(err, reply);
		if(!err){
			res.send(reply);	
		}else{
			res.status(404).send();
		}
	})
})


/**
 * url to get the count of battles.
 */
app.get('/count', function(req, res){
	toppr.getBattlesCount(db, function(err, reply){
		if(!err){
			res.json({count: reply});	
		}else{
			res.status(404).send();
		}
	})
})

/**
 * url to get the stats about battle data
 */
app.get('/stats', function(req, res){
	toppr.getStats(db, function(err, reply){
		if(!_.isEmpty(reply)){
			res.send(reply);
		}else{
			res.status(404).send();
		}
	})
})

/**
 * index page
 */
app.get('/', function(req, res){
	db.redisWrite.flushdb(function(err, reply){
		toppr.insertBattleData(db, function(err, reply){
			//console.log(err, reply);
			res.sendFile(__dirname + "/index.html");
		})	
	})
	
})


app.listen(4000);