/**
 *
 * Created By Raghav on 04/09/2016
 *
 */
var _ = require('lodash');

var toppr = {};
var _toppr = {};

/**
 * function to read data from MySQL table `battles`
 */
_toppr.readData = function(db, callback){
	db.mySql.query("SELECT * FROM battles", function(err, reply){
		//console.log(err, reply);
		callback(err, reply);
	})
}

// _toppr.readData(function(err, db, reply){
// 	console.log(err, reply[0].attacker_outcome);
// 	console.log(typeof reply);
// 	var obj = {};
// 	for(var key in reply[0]){
// 		console.log(key, reply[0][key]);
// 		if(reply[0][key] !== ''){
// 			obj[key] = reply[0][key];
// 		}
// 	}
// 	db.redisWrite.hmset("toppr_got_battle_" + reply[0].id, obj, function(err, reply){
// 		console.log(err, reply);
// 	})
// })


/**
 * helper function to insert battle data into redis. 
 */
_toppr.insertIntoRedis = function(mysqlReply, db, callback){
	var redisMulti = db.redisWrite.multi();

	if(mysqlReply.length === 0){
		return callback(false, true);
	}else{
		var obj = {};
		for(var key in mysqlReply[0]){
			if(mysqlReply[0][key] !== ''){
				obj[key] = mysqlReply[0][key];
			}
		}

		redisMulti.zadd("toppr_got_battles", mysqlReply[0].id, "toppr_got_battle_" + mysqlReply[0].id);
		redisMulti.hmset("toppr_got_battle_" + mysqlReply[0].id, obj);
		
		if(obj.attacker_king !== undefined){
			redisMulti.zincrby("toppr_got_attacker_king", 1, obj.attacker_king);	
		}
		if(obj.defender_king !== undefined){
			redisMulti.zincrby("toppr_got_defender_king", 1, obj.defender_king);	
		}
		if(obj.region !== undefined){
			redisMulti.zincrby("toppr_got_region", 1, obj.region);	
		}
		if(obj.name !== undefined){
			redisMulti.zincrby("toppr_got_name", 1, obj.name);	
		}
		if(obj.attacker_outcome !== undefined){
			redisMulti.zincrby("toppr_got_attacker_outcome", 1, obj.attacker_outcome);	
		}
		if(obj.battle_type !== undefined){
			redisMulti.sadd("toppr_got_battle_types", obj.battle_type);	
		}
		if(obj.defender_size !== undefined){
			redisMulti.zadd("toppr_got_defender_size", Number(obj.defender_size), "defender_size_" + obj.id);	
		}
		
		redisMulti.exec(function(err, reply){
			if(!err){
				mysqlReply.splice(0, 1);
				_toppr.insertIntoRedis(mysqlReply, db, function(err2, reply2){
					callback(err2, reply2);
				})
			}else{
				return callback(true, false);
			}
		})
		// db.redisWrite.zadd("toppr_got_battles", mysqlReply[0].id, "toppr_got_battle_" + mysqlReply[0].id, function(zsetErr, zsetReply){
		// 	if(!zsetErr){
		// 		db.redisWrite.hmset("toppr_got_battle_" + mysqlReply[0].id, obj, function(err, reply){
		// 			if(!err){
		// 				mysqlReply.splice(0, 1);
		// 				_toppr.insertIntoRedis(mysqlReply, db, function(err2, reply2){
		// 					callback(err2, reply2);
		// 				})
		// 			}else{
		// 				return callback(true, false);
		// 			}
		// 		})
		// 	}else{
		// 		return callback(true, false);
		// 	}
		// })
	}
}

/**
 * function to read data from MySQL and insert it into Redis.
 */
toppr.insertBattleData = function(db, callback){
	_toppr.readData(db, function(err, mysqlReply){
		if(!err){
			_toppr.insertIntoRedis(mysqlReply, db, function(redisErr, redisReply){
				callback(redisErr, redisReply);
			})
		}else{
			callback(true, false);
		}
	})
}	

/**
 * function to get the battle list.
 */
toppr.getBattlesList = function(db, callback){
	var redisMulti = db.redisRead.multi();

	db.redisRead.zrange("toppr_got_battles", 0, -1, function(err, battlesHashArray){
		if(!err){
			for(var i = 0; i < battlesHashArray.length; i++){
				redisMulti.hgetall(battlesHashArray[i]);
			}
			redisMulti.exec(function(err2, battleData){
				if(!err2){
					callback(false, battleData);
				}else{
					callback(true, false);
				}
			})
		}
	})
}


/**
 * function to get total no. of records
 */
toppr.getBattlesCount = function(db, callback){
	db.redisRead.zcard("toppr_got_battles", function(err, count){
		//console.log(err, count);
		if(!err){
			callback(false, count);
		}else{
			callback(true, false);
		}
	})
}

/**
 * function to get the most_active stats
 */
_toppr.getMostActiveStats = function(db, callback){
	var obj = {};
	db.redisRead.zrange("toppr_got_attacker_king", -1, -1, function(err, mostAttackerKing){
		if(!err){
			obj.attacker_king = mostAttackerKing[0];
		}
		db.redisRead.zrange("toppr_got_defender_king", -1, -1, function(err2, mostDefenderKing){
			if(!err2){
				obj.defender_king = mostDefenderKing[0];
			}
			db.redisRead.zrange("toppr_got_region", -1, -1, function(err3, regions){
				if(!err3){
					obj.region = regions[0];
				}
				db.redisRead.zrange("toppr_got_name", -1, -1, function(err4, names){
					if(!err4){
						obj.name = names[0];
					}
					callback(false, obj);
				})
			})
		})
	})
}

/**
 * function to get the attacker_outcome stats
 */
_toppr.getAttackerOutcomeStats = function(db, callback){
	var obj = {};
	db.redisRead.zrange("toppr_got_attacker_outcome", 0, -1, "withscores", function(err, reply){
		if(!err){
			for(var i = 0; i < reply.length; i+=2){
				obj[reply[i]] = reply[i+1];
			}
			callback(false, obj);
		}else{
			callback(true, false);
		}
	})
}

/**
 * function to get the battle_types stats
 */
_toppr.getBattleTypesStats = function(db, callback){
	db.redisRead.smembers("toppr_got_battle_types", function(err, battle_types){
		if(!err){
			callback(false, battle_types);
		}else{
			callback(true, false);
		}
	})
}

/**
 * function to get the defender_size stats
 */
_toppr.getDefenderSizeStats = function(db, callback){
	var obj = {};
	db.redisRead.zrange("toppr_got_defender_size", 0, -1, "withscores", function(err, size){
		if(!err){
			obj.min = size[1];
			obj.max = size[size.length - 1];
			var sum = 0;
			for(var i = 1; i < size.length; i+=2){
				sum += Number(size[i]);
			}
			obj.average = sum/(size.length/2);
			callback(false, obj);
		}else{
			callback(true, false);
		}
	})
}


/**
 * function to get the stats about the battle data.
 */
toppr.getStats = function(db, callback){
	var statsObj = {};
	_toppr.getMostActiveStats(db, function(err, most_active){
		if(!_.isEmpty(most_active)){
			statsObj.most_active = most_active;
		}
		_toppr.getAttackerOutcomeStats(db, function(err2, attacker){
			if(!err2){
				statsObj.attacker_outcome = attacker;
			}
			_toppr.getBattleTypesStats(db, function(err3, battleTypesArray){
				if(!err3){
					statsObj.battle_types = battleTypesArray;
				}
				_toppr.getDefenderSizeStats(db, function(err4, defender_size){
					if(!err4){
						statsObj.defender_size = defender_size;
					}
					callback(false, statsObj);
				})
			})
		})
	})
}

module.exports = toppr;

