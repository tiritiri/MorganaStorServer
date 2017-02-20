
var http = require("http");
var cookie = require("cookie");
var wsServer = require("websocket").server;
var wsClient = require("websocket").client;
var multiparty = require("multiparty");
var fs = require("fs");
var os = require("os");
var mime = require("mime");
var child_process = require("child_process");
var path = require("path");
var sqlite = require("sqlite3").verbose();
var crypto = require("crypto");

var loggerClass = (function(){

	function loggerClass(){
	}

	loggerClass.getTime = function(){
		var result = "";
		var date = new Date();
		result += date.getFullYear();
		result += "/";
		if(date.getMonth() + 1 < 10){
			result += "0";
		}
		result += date.getMonth() + 1;
		result += "/";
		if(date.getDate() < 10){
			result += "0";
		}
		result += date.getDate();
		result += "/";
		if(date.getHours() < 10){
			result += "0";
		}
		result += date.getHours();
		result += ":";
		if(date.getMinutes() < 10){
			result += "0";
		}
		result += date.getMinutes();
		result += ":";
		if(date.getSeconds() < 10){
			result += "0";
		}
		result += date.getSeconds();
		result += ".";
		if(date.getMilliseconds() < 100){
			result += "0";
		}
		if(date.getMilliseconds() < 10){
			result += "0";
		}
		result += date.getMilliseconds();
		return result;
	};

	loggerClass.log = function(title, body){
		console.log(loggerClass.getTime() + " INFO " + "[" + path.basename(require.main.filename) + "] " + title + "::" + body);
	};

	loggerClass.warn = function(title, body){
		console.warn(loggerClass.getTime() + " WARN " + "[" + path.basename(require.main.filename) + "] " + title + "::" + body);
	};

	loggerClass.error = function(title, body){
		console.error(loggerClass.getTime() + " ERROR " + "[" + path.basename(require.main.filename) + "] " + title + "::" + body);
	};

	loggerClass.spec = function(){
		var result = "";
		result += "[OS]:" + process.platform + "(" + process.arch + ")" + "\n";
		result += "[Version]:" + process.versions + "\n";
		result += "[SystemTotalMemory]:" + os.totalmem() + "\n";
		result += "[SystemFreeMemory]:" + os.freemem() + "\n";
		result += "[SystemUsedMemory]:" + os.totalmem() - os.freemem() + "\n";
		result += "[AppUsedMemory]:" + (process.memoryUsage())["rss"] + "\n";
		result += "[AppHeapTotalMemory]:" + (process.memoryUsage())["heapTotal"] + "\n";
		result += "[AppHeapUsedMemory]:" + (process.memoryUsage())["heapUsed"] + "\n";
		loggerClass.log("Status", "\n" + result);
	};

	return loggerClass;
})();


var dataStoreClass = (function(){

	function dataStoreClass(){
		this.name = null;
		this.nameHash = null;
		this.sql = null;
		this.path = dataStoreClass.basePath + "/data" + dataStoreClass.suffix + "/";
		this.storePath = null;
		this.blobPath = null;
	}

	dataStoreClass.basePath = __dirname;
	dataStoreClass.suffix = "";

	dataStoreClass.dataStoreList = {};

	dataStoreClass.setBasePath = function(path){
		dataStoreClass.basePath = path;
	};

	dataStoreClass.setSuffix = function(name){
		dataStoreClass.suffix = name;
	};

	dataStoreClass.getHandle = function(name, callback){
		if(dataStoreClass.dataStoreList[name]){
			dataStoreClass.dataStoreList[name]["lastTime"] = Date.now();
			if(callback){
				callback(true, dataStoreClass.dataStoreList[name]["store"]);
			}
		}else{
			var dataStore = new dataStoreClass();
			dataStoreClass.dataStoreList[name] = {};
			dataStoreClass.dataStoreList[name]["lastTime"] = Date.now();
			dataStoreClass.dataStoreList[name]["store"] = dataStore;
			dataStore.init(name, callback);
		}
		dataStoreClass.gcHandles();
	};

	dataStoreClass.gcHandles = function(){
		setTimeout(function(){
			for(var name in dataStoreClass.dataStoreList){
				var limitTime = Date.now() - (1000 * 60 * 15);
				if(dataStoreClass.dataStoreList[name]["lastTime"] < limitTime){
					dataStoreClass.gcHandle(name);
				}
			}
		}, 1);
	};

	dataStoreClass.gcHandle = function(name){
		loggerClass.log("dataStore", "SQL(" + name + ")切断");
		(dataStoreClass.dataStoreList[name]["store"]).sql.close();
		dataStoreClass.dataStoreList[name] = null;
		delete dataStoreClass.dataStoreList[name];
	};

	dataStoreClass.deleteStore = function(name, callback){
		try{
			var nameHash = dataStoreClass.getSha256(name);
			dataStoreClass.gcHandle(name);
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.makeHashDir = function(callback){
		var self = this;
		try{
			fs.mkdir(self.path, function(error){
				loggerClass.log("dataStore", "データ保存フォルダ(Base)作成:" + self.path);
				fs.mkdir(self.storePath, function(error){
					loggerClass.log("dataStore", "データ保存フォルダ(Data)作成:" + self.storePath);
					fs.mkdir(self.blobPath, function(error){
						loggerClass.log("dataStore", "データ保存フォルダ(Blob)作成:" + self.blobPath);
						if(callback){
							callback(true, null);
						}
					});
				});
			});
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.makeBlobInitDir = function(blobInitPath, callback){
		fs.exists(blobInitPath, function(exists){
			if(! exists){
				fs.mkdir(blobInitPath, function(error){
					loggerClass.log("dataStore", "データ保存フォルダ作成:" + blobInitPath);
					if(callback){
						callback(true, null);
					}
				});
			}else{
				if(callback){
					callback(true, null);
				}
			}
		});
	};

	dataStoreClass.prototype.init = function(name, callback){
		var self = this;
		try{
			this.name = name;
			this.nameHash = dataStoreClass.getSha256(name);
			this.storePath = this.path + this.nameHash + "/";
			this.blobPath = this.storePath + "blob/";
			this.makeHashDir(function(){
				var path = self.storePath + "index" + ".db";
				loggerClass.log("dataStore", "データベースを設定(" + path + ")");
				self.sql = new sqlite.Database(path);
				var sqlText = "CREATE TABLE IF NOT EXISTS primalIndex(key TEXT PRIMARY KEY, value BLOB, fileOuter INTEGER, timeStamp INTEGER, size INTEGER, meta TEXT)";
				self.sql.run(sqlText, [], function(error){
					if(! error){
						if(callback){
							callback(true, self);
						}
					}else{
						loggerClass.warn("dataStore", "init:" + error);
						if(callback){
							callback(false, error);
						}
					}
				});
			});
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.setItem = function(key, value, timeStamp, meta, callback){
		var self = this;
		try{
			var size = value.length;
			var sqlText = "INSERT OR REPLACE INTO primalIndex(key, value, fileOuter, timeStamp, size, meta) VALUES(?, ?, ?, ?, ?, ?)";
			this.sql.run(sqlText, [key, value, 0, timeStamp, size, meta], function(error){
				if(! error){
					self.removeItemDirect(key, function(status){
						if(callback){
							callback(true, null);
						}
					});
				}else{
					if(callback){
						callback(false, error);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.setBlob = function(key, blob, index, timeStamp, meta, callback){
		var self = this;
		try{
			var setRecord = function(size, callback){
				var sqlText = "INSERT OR REPLACE INTO primalIndex(key, value, fileOuter, timeStamp, size, meta) VALUES(?, ?, ?, ?, ?, ?)";
				self.sql.run(sqlText, [key, "", 1, timeStamp, size, meta], function(error){
					if(! error){
						if(callback){
							callback(true, null);
						}
					}else{
						if(callback){
							callback(false, error);
						}
					}
				});
			};
			var setData = function(callback){
				self.setBlobDirect(key, blob, index, function(status){
					if(status){
						if(callback){
							callback(true, null);
						}
					}else{
						if(callback){
							callback(false, null);
						}
					}
				});
			};
			if(blob){
				if(index == 0){
					setRecord(0, function(status, result){
						if(status){
							setData(callback);
						}else{
							if(callback){
								callback(false, null);
							}
						}
					});
				}else{
					setData(callback);
				}
			}else{
				setRecord(index, callback);
			}
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.setBlobDirect = function(key, blob, index, callback){
		try{
			var keyHash = dataStoreClass.getMd5(key);
			var blobInitPath = this.blobPath + keyHash.substr(0, 1) + "_" + keyHash.substr(-1, 1) + "/";
			this.makeBlobInitDir(blobInitPath, function(){
				var path = blobInitPath + keyHash + ".dat";
				var writeFile = function(blob, index){
					fs.open(path, "a", function(error, fd){
						fs.write(fd, blob, 0, blob.length, index, function(error, wByte, buffer){
							if(! error){
								fs.close(fd, function(error){
									if(! error){
										if(callback){
											callback(true, null);
										}
									}else{
										if(callback){
											callback(false, error);
										}
									}
								});
							}else{
								if(callback){
									callback(false, error);
								}
							}
						});
					});
				};
				if(index == 0){
					fs.unlink(path, function(error){
						writeFile(blob, index);
					});
				}else{
					writeFile(blob, index);
				}
			});
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.removeItem = function(key, callback){
		var self = this;
		try{
			var sqlText = "DELETE FROM primalIndex WHERE key = ?";
			this.sql.run(sqlText, [key], function(error){
				if(! error){
					self.removeItemDirect(key, function(status){
						if(status){
							if(callback){
								callback(true, null);
							}
						}else{
							if(callback){
								callback(false, null);
							}
						}
					});
				}else{
					if(callback){
						callback(false, error);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.removeItemDirect = function(key, callback){
		try{
			var keyHash = dataStoreClass.getMd5(key);
			var path = this.blobPath + keyHash.substr(0, 1) + "_" + keyHash.substr(-1, 1) + "/" + keyHash + ".dat";
			fs.exists(path, function(exists){
				if(exists){
					fs.unlink(path, function(error){
						if(callback){
							callback(true, null);
						}
					});
				}else{
					if(callback){
						callback(true, null);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.getItem = function(key, callback){
		var self = this;
		try{
			var sqlText = "SELECT key, value, timeStamp, size, fileOuter, meta FROM primalIndex WHERE key = ? LIMIT 1";
			this.sql.all(sqlText, [key], function(error, rows){
				if((! error) && 1 == rows.length){
					if(rows[0]["fileOuter"] == 1){
						self.getItemDirectStream(key, function(status, stream){
							if(status){
								if(callback){
									callback(true, rows[0], stream);
								}
							}else{
								if(callback){
									callback(false, rows[0], null);
								}
							}
						});
					}else{
						if(callback){
							callback(true, rows[0], null);
						}
					}
				}else{
					if(callback){
						callback(false, null, null);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, error, null);
			}
		}
	};

	dataStoreClass.prototype.getItemDirectStream = function(key, callback){
		try{
			var keyHash = dataStoreClass.getMd5(key);
			var path = this.blobPath + keyHash.substr(0, 1) + "_" + keyHash.substr(-1, 1) + "/" + keyHash + ".dat";
			fs.exists(path, function(exists){
				if(exists){
					var readStream = fs.createReadStream(path);
					if(callback){
						callback(true, readStream);
					}
				}else{
					if(callback){
						callback(false, null);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, null);
			}
		}
	};

	dataStoreClass.prototype.getKeys = function(offset, limit, callback){
		var self = this;
		try{
			var sqlText = "SELECT key, size, timeStamp, fileOuter FROM primalIndex LIMIT ?, ?";
			this.sql.all(sqlText, [offset, limit], function(error, rows){
				if((! error) && rows){
					if(callback){
						callback(true, rows);
					}
				}else{
					if(callback){
						callback(false, null);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, null);
			}
		}
	};

	dataStoreClass.prototype.countKeys = function(callback){
		try{
			var sqlText = "SELECT COUNT(key) AS 'count' FROM primalIndex";
			this.sql.all(sqlText, [], function(error, rows){
				if((! error) && 1 == rows.length){
					if(callback){
						callback(true, rows[0]["count"]);
					}
				}else{
					if(callback){
						callback(false, 0);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, null);
			}
		}
	};

	dataStoreClass.prototype.existsItem = function(key, callback){
		try{
			var sqlText = "SELECT key FROM primalIndex WHERE key = ? LIMIT 1";
			this.sql.all(sqlText, [key], function(error, rows){
				if(! error){
					if(1 == rows.length){
						if(callback){
							callback(true, true);
						}
					}else{
						if(callback){
							callback(true, false);
						}
					}
				}else{
					if(callback){
						callback(false, false);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, error);
			}
		}
	};

	dataStoreClass.prototype.likeKeys = function(key, offset, limit, callback){
		try{
			var sqlText = "SELECT key, size, timeStamp, fileOuter FROM primalIndex WHERE key LIKE ? LIMIT ?, ?";
			this.sql.all(sqlText, [key, offset, limit], function(error, rows){
				if((! error) && rows){
					if(callback){
						callback(true, rows);
					}
				}else{
					if(callback){
						callback(false, null);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, null);
			}
		}
	};

	dataStoreClass.prototype.likeItems = function(key, offset, limit, callback){
		try{
			var sqlText = "SELECT key, value, timeStamp FROM primalIndex WHERE fileOuter <> 1 AND key LIKE ? LIMIT ?, ?";
			this.sql.all(sqlText, [key, offset, limit], function(error, rows){
				if((! error) && rows){
					if(callback){
						callback(true, rows);
					}
				}else{
					if(callback){					
						callback(false, null);
					}
				}
			});
		}catch(error){
			if(callback){
				callback(false, null);
			}
		}
	};

	dataStoreClass.prototype.countLikeKeys = function(key, callback){
		try{
			var sqlText = "SELECT COUNT(key) AS 'count' FROM primalIndex WHERE key LIKE ?";
			this.sql.all(sqlText, [key], function(error, rows){
				if((! error) && 1 == rows.length){
					if(callback){					
						callback(true, rows[0]["count"]);
					}
				}else{
					if(callback){					
						callback(false, 0);
					}
				}
			});
		}catch(error){
			if(callback){			
				callback(false, null);
			}
		}
	};

	dataStoreClass.getSha256 = function(src){
		try{
			var sha = crypto.createHash("sha256");
			sha.update(src);
			return sha.digest("hex");
		}catch(error){
		}
		return null;
	};

	dataStoreClass.getMd5 = function(src){
		try{
			var md5 = crypto.createHash("md5");
			md5.update(src);
			return md5.digest("hex");
		}catch(error){
		}
		return null;
	};
	
	return dataStoreClass;
})();




var MorganaClusterQueClass = (function(){

	function MorganaClusterQueClass(responseHandle, responseType, responseMode, responseHeader){
		this.responseHandle = responseHandle;

		this.responseHeader = responseHeader;
		this.requestHandles = {};
		this.responsed = null;
		this.responseCount = 0;
		if(responseMode == "fast"){
			this.responseMode = responseMode;
			switch(responseType){
				default:
					throw "responseType Error";
					//break;
				case "bool":
					this.responseType = responseType;
					break;
				case "value":
					this.responseType = responseType;
					break;
				case "object":
					this.responseType = responseType;
					break;
			}
		}else if(responseMode == "integrity"){
			this.responseMode = responseMode;
			switch(responseType){
				default:
					throw "responseType Error";
					//break;
				case "bool":
					this.responseType = responseType;
					break;
				case "value":
					this.responseType = responseType;
					break;
			}
		}else if(responseMode == "merge"){
			this.responseType = "object";
		}else{
			throw "responseMode Error(" + responseMode + ")";
		}
	}

	MorganaClusterQueClass.prototype.addRequest = function(requestHost, requestPort, requestPath, requestMethod){
		var self = this;
		var handleID = Date.now() + "-" + String(Math.random()).substr(2);
		var options = {
			hostname: requestHost,
			port: requestPort,
			path: requestPath,
			method: requestMethod
		};
		var request = http.request(options, function(response){
			if(self.responseMode == "fast"){
				self.responseFast(handleID, response);
			}else{
				switch(self.responseMode + "-" + self.responseType){
					case "integrity-bool":
						self.responseIntegrityBool(handleID, response);
						break;
					case "integrity-value":
						self.responseIntegrityValue(handleID, response);
						break;
					case "merge-object":
						self.responseMergeObject(handleID, response);
						break;
				}
			}
		});
		request.setTimeout(15 * 60 * 1000);
		request.on("timeout", function(){
			request.abort();
			loggerClass.log("MorganaClusterQue", "ClusterTimeoutError");
			if(self.responseMode == "fast"){
				self.responseFast(handleID, null);
			}else{
				switch(self.responseMode + "-" + self.responseType){
					case "integrity-bool":
						self.responseIntegrityBool(handleID, null);
						break;
					case "integrity-value":
						self.responseIntegrityValue(handleID, null);
						break;
					case "merge-object":
						self.responseMergeObject(handleID, null);
						break;
				}
			}
		});
		request.on("error", function(error){
			loggerClass.log("MorganaClusterQue", "ClusterAccessError[" + error.code + "]:" + error.message);
			if(self.responseMode == "fast"){
				self.responseFast(handleID, null);
			}else{
				switch(self.responseMode + "-" + self.responseType){
					case "integrity-bool":
						self.responseIntegrityBool(handleID, null);
						break;
					case "integrity-value":
						self.responseIntegrityValue(handleID, null);
						break;
					case "merge-object":
						self.responseMergeObject(handleID, null);
						break;
				}
			}
		});
		this.requestHandles[handleID] = {};
		this.requestHandles[handleID]["request"] = request;
	};

	MorganaClusterQueClass.prototype.responseFast = function(handleID, response){
		this.responseCount ++;
		if(response){
			this.requestHandles[handleID]["response"] = response;
			if((! this.responsed) && response.statusCode == 200){
				this.responsed = handleID;
				this.responseHeader["X-responseType"] = this.responseType;
				this.responseHandle.writeHead(200, this.responseHeader);
				response.pipe(this.responseHandle);
			}else{
				response.abort();
			}
		}
		if(this.responseCount <= Object.keys(this.requestHandles).length){
			if(! this.responsed){
				this.responseHandle.writeHead(404, {"Content-Type":"text/html"});
				this.responseHandle.end("ERROR::Data Not Found");
			}
			for(var key in this.requestHandles){
				delete this.requestHandles[key];
			}
		}
	};

	MorganaClusterQueClass.prototype.responseIntegrityBool = function(handleID, response){
		if(response){
			this.requestHandles[handleID]["response"] = response;
			var data = "";
			response.on("data", function(chunk){
				data += chunk;
			});
			response.on("end", function(){
				this.responseCount ++;
				if(data == "true"){
					if(! this.responsed){
						this.responsed = handleID;
						this.responseHeader["X-responseType"] = this.responseType;
						this.responseHandle.writeHead(200, this.responseHeader);
						this.responseHandle.end("true");
					}
				}
				if(Object.keys(this.requestHandles).length <= this.responseCount){
					if(! this.responsed){
						this.responsed = handleID;
						this.responseHeader["X-responseType"] = this.responseType;
						this.responseHandle.writeHead(200, this.responseHeader);
						this.responseHandle.end("false");
					}
					for(var key in this.requestHandles){
						delete this.requestHandles[key];
					}
				}
			});
		}else{
			this.responseCount ++;
			if(Object.keys(this.requestHandles).length <= this.responseCount){
				if(! this.responsed){
					this.responseHandle.writeHead(404, {"Content-Type":"text/html"});
					this.responseHandle.end("ERROR::Data Not Found");
				}
				for(var key in this.requestHandles){
					delete this.requestHandles[key];
				}
			}
		}
	};

	MorganaClusterQueClass.prototype.responseIntegrityValue = function(handleID, response){
		this.responseCount ++;
		if(response){
			this.requestHandles[handleID]["response"] = response;
			if(response.statusCode == 200){
				this.requestHandles[handleID]["timeStamp"] = parseInt(response.headers["x-timeStamp"]);
				response.pause();
			}else{
				response.abort();
			}
			if(this.responseCount <= Object.keys(this.requestHandles).length){
				if(! this.responsed){
					for(var key in this.requestHandles){
						if((! this.responsed) || this.requestHandles[this.responsed]["timeStamp"] < this.requestHandles[key]["timeStamp"]){
							this.responsed = key;
						}
					}
					this.responseHeader["X-responseType"] = this.responseType;
					this.responseHandle.writeHead(200, this.responseHeader);
					this.requestHandles[this.responsed]["response"].pipe(this.responseHandle);
					this.requestHandles[this.responsed]["response"].resume();
				}

			}
		}
		if(this.responseCount <= Object.keys(this.requestHandles).length){
			if(! this.responsed){
				this.responseHandle.writeHead(404, {"Content-Type":"text/html"});
				this.responseHandle.end("ERROR::Data Not Found");
			}
			for(var key in this.requestHandles){
				if(this.responsed != key){
					try{
						this.requestHandles[handleID]["response"].abort();
					}catch(error){
					}
				}
				delete this.requestHandles[key];
			}
		}
	};

	MorganaClusterQueClass.prototype.responseMergeObject = function(handleID, response){
		if(response && response.statusCode == 200){
			this.requestHandles[handleID]["response"] = response;
			var data = "";
			response.on("data", function(chunk){
				data += chunk;
			});
			response.on("end", function(){
				this.responseCount ++;
				try{
					this.requestHandles[handleID]["result"] = JSON.parse(data);
				}catch(error){
				}
				if(Object.keys(this.requestHandles).length <= this.responseCount){
					if(! this.responsed){
						this.responsed = handleID;
						this.responseHeader["X-responseType"] = this.responseType;
						this.responseHandle.writeHead(200, this.responseHeader);
						var responseBody = {};
						for(var key in this.requestHandles){
							if(this.requestHandles[key]["result"]){
								for(var key in this.requestHandles[key]["result"]){
									if((! responseBody) || responseBody[key]["timeStamp"] < this.requestHandles[key]["result"][key]["timeStamp"]){
										responseBody[key] = this.requestHandles[key]["result"][key];
									}
								}
							}
						}
						this.responseHandle.end(JSON.stringify(responseBody));
					}
					for(var key in this.requestHandles){
						delete this.requestHandles[key];
					}
				}
			});
		}else{
			this.responseCount ++;
			if(Object.keys(this.requestHandles).length <= this.responseCount){
				if(! this.responsed){
					this.responseHandle.writeHead(404, {"Content-Type":"text/html"});
					this.responseHandle.end("ERROR::Data Not Found");
				}
				for(var key in this.requestHandles){
					delete this.requestHandles[key];
				}
			}
		}
	};

	MorganaClusterQueClass.prototype.sendWrite = function(data){
		for(var key in this.requestHandles){
			this.requestHandles[key]["request"].write(data);
		}
	};

	MorganaClusterQueClass.prototype.sendEnd = function(){
		for(var key in this.requestHandles){
			this.requestHandles[key]["request"].end();
		}
	};

	MorganaClusterQueClass.prototype.abort = function(){
		for(var key in this.requestHandles){
			this.requestHandles[key]["request"].abort();
		}
	};

	return MorganaClusterQueClass;
})();




var MorganaFunctionClass = (function(){

	function MorganaFunctionClass(){
	}

	MorganaFunctionClass.urlParse = function(url){
		var result = {};
		var pathText = null;
		var searchText = null;
		try{
			if(0 <= url.indexOf("?")){
				pathText = url.substring(0, url.indexOf("?"));
				searchText = url.substr(url.indexOf("?") + 1);
			}else{
				pathText = url;
			}
			result["path"] = pathText.split("/");
			for(var i = 0; i < result["path"].length; i++){
				result["path"][i] = decodeURIComponent(result["path"][i]);
			}
			result["get"] = MorganaFunctionClass.parseKeyValueText(searchText, "&", "=");
			result["search"] = searchText;
		}catch(error){

		}
		return result;
	};

	MorganaFunctionClass.clearOneItemArray = function(object){
		if(object instanceof Array){
			var length = object.length;
			if(length == 0){
				return null;
			}else if(length == 1){
				return MorganaFunctionClass.clearOneItemArray(object[0]);
			}else{
				for(var i = 0; i < length; i++){
					object[i] = MorganaFunctionClass.clearOneItemArray(object[i]);
				}
				return object;
			}
		}else if(object instanceof Object){
			var keys = Object.keys(object);
			var length = keys.length;
			if(length == 0){
				return null;
			}else if(length == 1){
				return MorganaFunctionClass.clearOneItemArray(object[keys[0]]);
			}else{
				for(var key in object){
					object[key] = MorganaFunctionClass.clearOneItemArray(object[key]);
				}
				return object;
			}
		}else{
			return object;
		}
	};
	
	MorganaFunctionClass.parseKeyValueText = function(text, rowSplit, keyValueSplit){
		var result = {};
		try{
			var kv = text.split(rowSplit);
			for(var i = 0; i < kv.length; i++){
				var temp = kv[i].split(keyValueSplit);
				if(! result[decodeURIComponent(temp[0])]){
					result[decodeURIComponent(temp[0])] = [];
				}
				result[decodeURIComponent(temp[0])].push(decodeURIComponent(temp[1]));
			}
		}catch(error){
			
		}
		return result;
	};

	return MorganaFunctionClass;

})();




var MorganaStorageClass = (function(){

	function MorganaStorageClass(){
		this.nodeUID = "Storage" + "::" + Date.now() + "-" + String(Math.random()).substr(2);
		this.wsSessionList = {};
		this.serverPort = 8081;
		this.httpServer = null;
		this.websocketServer = null;
	}
	
	MorganaStorageClass.prototype.stop = function(){
		try{
			this.websocketServer.close();
		}catch(error){

		}
		try{
			this.httpServer.close();
		}catch(error){

		}
		loggerClass.log("MorganaStorage", "HTTP STOP");
	};

	MorganaStorageClass.prototype.start = function(port, dataDir){
		if(isFinite(port)){
			dataStoreClass.setBasePath(dataDir);
			dataStoreClass.setSuffix("_" + port);
			this.serverPort = parseInt(port);
			var self = this;
			loggerClass.log("MorganaStorage", "HTTP Listen(" + this.serverPort + ")");
			loggerClass.log("MorganaStorage", "NodeUID(" + this.nodeUID + ")");
			this.httpServer = http.createServer(function(request, response){
				self.onHttpRequest(request, response);
			}).listen(this.serverPort);

			this.websocketServer = new wsServer({
				"httpServer": this.httpServer,
				"autoAcceptConnections": false
			});
			this.websocketServer.on("request",  function(request, response){
				self.onWebscoketRequest(request, response);
			});
		}else{
			loggerClass.log("MorganaStorage", "[No Storage Mode]");
		}
	};

	MorganaStorageClass.prototype.apiRequest = function(request, response, urlPath, cookies){
		try{
			if(this.receiveMessageAction[urlPath["path"][2]]){
				this.receiveMessageAction[urlPath["path"][2]].call(this, request, response, urlPath, cookies);
			}else{
				response.writeHead(400, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("ERROR::Bad Request");
			}
		}catch(error){
			response.writeHead(404, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
			response.end("Error");
			loggerClass.warn("MorganaStorage", "CommandError[" + request.method + "](" + request.url + "):" + error);
		}
	};

	MorganaStorageClass.prototype.apiGetInfoRequest = function(request, response, urlPath, cookies){
		var result = {
			"nodeInfo": {
				"nodeType": "storage",
				"nodeUID": this.nodeUID,
				"wsSessionList": Object.keys(this.wsSessionList),
			},
			"cpuInfo": {
				"SystemTotalMemory": os.totalmem(),
				"SystemFreeMemory": os.freemem(),
				"SystemUsedMemory": os.totalmem() - os.freemem(),
				"AppUsedMemory": (process.memoryUsage())["rss"],
				"AppHeapTotalMemory": (process.memoryUsage())["heapTotal"],
				"AppHeapUsedMemory": (process.memoryUsage())["heapUsed"]
			}
		};
		response.writeHead(200, {"Content-Type":"application/json"});
		response.end(JSON.stringify(result));
	};

	MorganaStorageClass.prototype.onHttpRequest = function(request, response){
		try{
			var urlPath = MorganaFunctionClass.urlParse(request.url);
			var cookies = {};
			if(request.headers.cookie){
				cookies = cookie.parse(request.headers.cookie);
			}
			switch(urlPath["path"][1]){
				default:
					throw new Error("Unknown Path");
					//break;
				case "api":
					this.apiRequest(request, response, urlPath, cookies);
					break;
				case "info":
					this.apiGetInfoRequest(request, response, urlPath, cookies);
					break;
			}
		}catch(error){
			response.writeHead(404, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
			response.end("Error");
			loggerClass.warn("MorganaStorage", "onRequest[" + request.method + "](" + request.url + "):" + error);
		}
	};

	MorganaStorageClass.prototype.onWebscoketRequest = function(request, response){
		//if(! this.originIsAllowed(request.origin)){
			//request.reject();
			//return;
		//}
		var self = this;
		var connection = request.accept("cluster-communication-protocol", request.origin);
		var sessionName = request.socket._peername.address.replace("::ffff:", "") + ":" + request.socket._peername.port;
		if(! this.wsSessionList[sessionName]){
			this.wsSessionList[sessionName] = connection;
			loggerClass.log("MorganaStorage", "ノード(クライアント:" + sessionName + ")と接続しました");
			loggerClass.log("MorganaStorage", "現在の接続ノード数:" + Object.keys(self.wsSessionList).length);
			connection.on("message", function(message){
				self.receiveMessage(message, connection, sessionName);
			});
			connection.on("close", function(reasonCode, description){
				loggerClass.log("MorganaStorage", "ノード(クライアント:" + sessionName + ")と切断しました");
				self.onWebscoketClose(sessionName);
			});
		}else{
			loggerClass.log("MorganaStorage", "ノード(クライアント:" + sessionName + ")との2重接続を抑止します");
			connection.close();
		}
	};

	MorganaStorageClass.prototype.onWebscoketClose = function(sessionName){
		if(this.wsSessionList[sessionName]){
			delete this.wsSessionList[sessionName];
			loggerClass.log("MorganaStorage", "現在の接続ノード数:" + Object.keys(this.wsSessionList).length);
		}
	};

	MorganaStorageClass.prototype.receiveMessage = function(message, connection, sessionName){
		try{
			//message.type == "utf8"
			//message.type == "binary"
			//message.binaryData
			//sendBytes
			var data = JSON.parse(message.utf8Data);
			if(this.receiveCtrlMessageAction[data["call"]]){
				this.receiveCtrlMessageAction[data["call"]].call(this, data, connection, sessionName);
			}else{
				loggerClass.log("MorganaStorage", "ノード(" + sessionName + ")から未知のコマンド(" + data["call"] + ")を受信しました");
			}
		}catch(error){
			loggerClass.log("MorganaStorage", "メッセージ処理失敗:" + error);
		}
	};

	MorganaStorageClass.prototype.receiveCtrlMessageAction = {};

	MorganaStorageClass.prototype.receiveCtrlMessageAction.getNodeInfo = function(requestData, connection, sessionName){
		if(requestData["nodeUID"] && requestData["nodeUID"] != this.nodeUID){
			connection.sendUTF(JSON.stringify({"call": "reNodeInfo", "nodeUID": this.nodeUID, "nodeType": "storage"}));
		}else{
			loggerClass.log("MorganaStorage", "reNodeInfo:nodeUID(" + this.nodeUID + ")が一致するため切断(" + sessionName + ")");
			connection.close();
		}
	};

	MorganaStorageClass.prototype.receiveCtrlMessageAction.reNodeInfo = function(requestData, connection, sessionName){
		if(requestData["nodeUID"] && requestData["nodeUID"] != this.nodeUID){
		}else{
			loggerClass.log("MorganaStorage", "reNodeInfo:nodeUID(" + this.nodeUID + ")が一致するため切断(" + sessionName + ")");
			connection.close();
		}
	};

	MorganaStorageClass.prototype.receiveCtrlMessageAction.broadcastMessage = function(requestData, connection, sessionName){
		loggerClass.log("MorganaStorage", "メッセージを配信:" + Object.keys(this.wsSessionList).length);
		this.sendMessageAll(requestData["message"]);
	};

	MorganaStorageClass.prototype.receiveCtrlMessageAction.heartBeat = function(requestData, connection){
		connection.sendUTF(JSON.stringify({"call": "reHeartBeat", "nodeUID": this.nodeUID}));
	};

	MorganaStorageClass.prototype.sendMessageAll = function(message){
		for(var sessionName in this.wsSessionList){
			this.wsSessionList[sessionName].sendUTF(JSON.stringify(message));
		}
	};

	MorganaStorageClass.prototype.receiveMessageAction = {};

	MorganaStorageClass.prototype.receiveMessageAction.nodeGetItem = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				store.getItem(requestInfo["key"], function(status, data, stream){
					if(status){
						var meta = JSON.parse(data["meta"]);
						var responseHeader = {
							"Content-Type": meta["Content-Type"],
							"Access-Control-Allow-Origin": "*",
							"X-timeStamp": data["timeStamp"],
							"X-size": data["size"],
							"X-requestCall": "nodeGetItem",
							"X-nodeUID": self.nodeUID,
							"X-table": requestInfo["table"],
							"X-responseType": "value"
						};
						if(stream){
							responseHeader["X-chunk-stream"] = "true";
							response.writeHead(200, responseHeader);
							stream.pipe(response);
						}else{
							responseHeader["X-chunk-stream"] = "false";
							response.writeHead(200, responseHeader);
							response.end(data["value"]);
						}
					}else{
						response.writeHead(404, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Data Not Found");
					}
				});
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};

	MorganaStorageClass.prototype.receiveMessageAction.nodeExistsItem = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				store.existsItem(requestInfo["key"], function(status, exists){
					if(status){
						var responseHeader = {
							"Content-Type": "text/html",
							"Access-Control-Allow-Origin": "*",
							"X-requestCall": "nodeExistsItem",
							"X-nodeUID": self.nodeUID,
							"X-table": requestInfo["table"],
							"X-responseType": "bool"
						};
						if(exists){
							response.writeHead(200, responseHeader);
							response.end("true");
						}else{
							response.writeHead(404, responseHeader);
							response.end("false");
						}
					}else{
						response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Store Response Fail");
					}
				});
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};

	MorganaStorageClass.prototype.receiveMessageAction.nodeSetItem = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		var timeStamp = Math.floor(Date.now() / 1000);
		var responseHeader = {
			"Content-Type": "text/html",
			"Access-Control-Allow-Origin": "*",
			"X-requestCall": "nodeSetItem",
			"X-nodeUID": self.nodeUID,
			"X-table": requestInfo["table"],
			"X-responseType": "bool"
		};
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				if(request.method == "GET"){
					var meta = {
						"Content-Type": "text/plane"
					};
					store.setItem(requestInfo["key"], requestInfo["value"], timeStamp, JSON.stringify(meta), function(status){
						if(status){
							response.writeHead(200, responseHeader);
							response.end("true");
							self.sendMessageAll({
								"call": "notify",
								"nodeUID": self.nodeUID,
								"message": {
									"type": "nodeSetItem",
									"table": requestInfo["table"],
									"key": requestInfo["key"]
								}
							});
						}else{
							response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
							response.end("ERROR::Store Response Fail");
						}
					});
				}else if(request.method == "POST"){
					var itemLoadedLength = 0;
					var meta = {};
					try{
						meta["Content-Type"] = mime.lookup(requestInfo["meta"]);
					}catch(error){
						meta["Content-Type"] = "application/octet-stream";
					}
					request.on("data", function(chunk){
						request.pause();
						store.setBlob(requestInfo["key"], chunk, itemLoadedLength, timeStamp, JSON.stringify(meta), function(status){
							if(status){
								itemLoadedLength += chunk.length;
							}else{
								response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
								response.end("ERROR::Store Response Fail");
							}
							request.resume();
						});
					});
					request.on("end", function(){
						store.setBlob(requestInfo["key"], null, itemLoadedLength, timeStamp, JSON.stringify(meta), function(status){
							if(status){
								response.writeHead(200, responseHeader);
								response.end("true");
								self.sendMessageAll({
									"call": "notify",
									"nodeUID": self.nodeUID,
									"message": {
										"type": "nodeSetItem",
										"table": requestInfo["table"],
										"key": requestInfo["key"]
									}
								});
							}else{
								response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
								response.end("ERROR::Store Response Fail");
							}
						});
					});
					request.on("error", function(error){
						response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Store Response Fail");
					});
				}else{
					response.writeHead(400, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
					response.end("ERROR::Bad Request");
				}
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};

	MorganaStorageClass.prototype.receiveMessageAction.nodeRemoveItem = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				store.removeItem(requestInfo["key"], function(status){
					if(status){
						response.writeHead(200, {
							"Content-Type": "text/html",
							"Access-Control-Allow-Origin": "*",
							"X-requestCall": "nodeRemoveItem",
							"X-nodeUID": self.nodeUID,
							"X-table": requestInfo["table"],
							"X-responseType": "bool"
						});
						response.end("true");
						self.sendMessageAll({
							"call": "notify",
							"nodeUID": self.nodeUID,
							"message": {
								"type": "nodeRemoveItem",
								"table": requestInfo["table"],
								"key": requestInfo["key"]
							}
						});
					}else{
						response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Store Response Fail");
					}
				});
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};

	MorganaStorageClass.prototype.receiveMessageAction.nodeSearchKeys = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				store.likeKeys(requestInfo["key"], requestInfo["offset"], requestInfo["limit"], function(status, _data){
					var data = {};
					if(status){
						for(var i = 0; i < _data.length; i++){
							data[_data[i]["key"]] = _data[i];
						}
						response.writeHead(200, {
							"Content-Type": "text/html",
							"Access-Control-Allow-Origin": "*",
							"X-requestCall": "nodeSearchKeys",
							"X-nodeUID": self.nodeUID,
							"X-table": requestInfo["table"],
							"X-responseType": "object"
						});
						response.end(JSON.stringify(data));
					}else{
						response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Store Response Fail");
					}
				});
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};

	MorganaStorageClass.prototype.receiveMessageAction.nodeCountSearchKeys = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				store.countLikeKeys(requestInfo["key"], function(status, data){
					if(status){
						response.writeHead(200, {
							"Content-Type": "text/html",
							"Access-Control-Allow-Origin": "*",
							"X-requestCall": "nodeCountSearchKeys",
							"X-nodeUID": self.nodeUID,
							"X-table": requestInfo["table"],
							"X-responseType": "number"
						});
						response.end(data);
					}else{
						response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Store Response Fail");
					}
				});
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};

	MorganaStorageClass.prototype.receiveMessageAction.nodeGetKeys = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				store.getKeys(requestInfo["offset"], requestInfo["limit"], function(status, _data){
					var data = {};
					if(status){
						for(var i = 0; i < _data.length; i++){
							data[_data[i]["key"]] = _data[i];
						}
						response.writeHead(200, {
							"Content-Type": "text/html",
							"Access-Control-Allow-Origin": "*",
							"X-requestCall": "nodeGetKeys",
							"X-nodeUID": self.nodeUID,
							"X-table": requestInfo["table"],
							"X-responseType": "object"
						});
						response.end(JSON.stringify(data));
					}else{
						response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Store Response Fail");
					}
				});
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};

	MorganaStorageClass.prototype.receiveMessageAction.nodeCountKeys = function(request, response, urlPath, cookies){
		var self = this;
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		dataStoreClass.getHandle("#user@" + requestInfo["table"], function(status, store){
			if(status){
				store.countKeys(function(status, data){
					if(status){
						response.writeHead(200, {
							"Content-Type": "text/html",
							"Access-Control-Allow-Origin": "*",
							"X-requestCall": "nodeCountKeys",
							"X-nodeUID": self.nodeUID,
							"X-table": requestInfo["table"],
							"X-responseType": "number"
						});
						response.end(data);
					}else{
						response.writeHead(500, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
						response.end("ERROR::Store Response Fail");
					}
				});
			}else{
				response.writeHead(503, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("Error::Data Store Fail");
			}
		});
	};
	
	return MorganaStorageClass;
})();




var MorganaControllerClass = (function(){

	function MorganaControllerClass(){
		this.nodeUID = "Controller" + "::" + Date.now() + "-" + String(Math.random()).substr(2);
		this.activeClusterList = {};
		this.standbyClusterList = {};
		this.wsSessionList = {};
		this.serverPort = 8080;
		this.httpServer = null;
		this.websocketServer = null;
		this.clusterPolicy = {
			"shadowCopyNodes": 0.3
		};
	}
	
	MorganaControllerClass.prototype.stop = function(){
		try{
			this.httpServer.close();
		}catch(error){

		}
		loggerClass.log("MorganaController", "HTTP STOP");
	};

	MorganaControllerClass.prototype.start = function(port, localConnectionNode, firstConnectionNode){
		var self = this;
		if(isFinite(port)){
			this.serverPort = parseInt(port);
			loggerClass.log("MorganaController", "HTTP Listen(" + this.serverPort + ")");
			loggerClass.log("MorganaController", "NodeUID(" + this.nodeUID + ")");
			this.httpServer = http.createServer(function(request, response){
				self.onHttpRequest(request, response);
			}).listen(this.serverPort);

			this.websocketServer = new wsServer({
				"httpServer": this.httpServer,
				"autoAcceptConnections": false
			});
			this.websocketServer.on("request",  function(request, response){
				self.onWebscoketRequest(request, response);
			});
			this.standbyClusterList[localConnectionNode] = true;
			if(firstConnectionNode){
				this.standbyClusterList[firstConnectionNode] = true;
			}
			this.getClusterNodeChache();
			this.connectionCycleWorker();
			setInterval(this.connectionCycleWorker, 1000 * 300);
		}else{
			loggerClass.log("MorganaController", "[No Controller Mode]");
		}
	};

	MorganaControllerClass.prototype.getClusterNodeChache = function(){
		var self = this;
		dataStoreClass.getHandle("#system@nodeInfo", function(status, store){
			if(status){
				var getListData = function(offset){
					store.getKeys(offset, 100, function(status, data){
						if(status){
							for(var i = 0; i < data.length; i++){
								self.standbyClusterList[data[i]["key"]] = true;
							}
							if(data.length == 100){
								getListData(offset + 100);
							}else{
								self.connectionCycleWorker();
							}
						}else{
							self.connectionCycleWorker();
						}
					});
				};
				getListData(0);
			}
		});
	};

	MorganaControllerClass.prototype.apiRequest = function(request, response, urlPath, cookies){
		try{
			if(this.receiveMessageAction[urlPath["path"][2]]){
				this.receiveMessageAction[urlPath["path"][2]].call(this, request, response, urlPath, cookies);
			}else{
				response.writeHead(400, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
				response.end("ERROR::Bad Request");
			}
		}catch(error){
			response.writeHead(404, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
			response.end("Error");
			loggerClass.warn("MorganaController", "CommandError[" + request.method + "](" + request.url + "):" + error);
		}
	};

	MorganaControllerClass.prototype.apiGetInfoRequest = function(request, response, urlPath, cookies){
		var result = {
			"nodeInfo": {
				"nodeType": "controller",
				"nodeUID": this.nodeUID,
				"wsSessionList": Object.keys(this.wsSessionList),
				"activeClusterList": Object.keys(this.activeClusterList)
			},
			"cpuInfo": {
				"SystemTotalMemory": os.totalmem(),
				"SystemFreeMemory": os.freemem(),
				"SystemUsedMemory": os.totalmem() - os.freemem(),
				"AppUsedMemory": (process.memoryUsage())["rss"],
				"AppHeapTotalMemory": (process.memoryUsage())["heapTotal"],
				"AppHeapUsedMemory": (process.memoryUsage())["heapUsed"]
			}
		};
		response.writeHead(200, {"Content-Type":"application/json", "Access-Control-Allow-Origin": "*"});
		response.end(JSON.stringify(result));
	};

	MorganaControllerClass.prototype.onHttpRequest = function(request, response){
		try{
			var urlPath = MorganaFunctionClass.urlParse(request.url);
			var cookies = {};
			if(request.headers.cookie){
				cookies = cookie.parse(request.headers.cookie);
			}
			switch(urlPath["path"][1]){
				default:
					throw new Error("Unknown Path");
					//break;
					break;
				case "api":
					this.apiRequest(request, response, urlPath, cookies);
					break;
				case "info":
					this.apiGetInfoRequest(request, response, urlPath, cookies);
					break;
			}
		}catch(error){
			response.writeHead(404, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
			response.end("Error");
			loggerClass.warn("MorganaController", "onRequest[" + request.method + "](" + request.url + "):" + error);
		}
	};

	MorganaControllerClass.prototype.onWebscoketRequest = function(request, response){
		//if(! this.originIsAllowed(request.origin)){
			//request.reject();
			//return;
		//}
		var self = this;
		var connection = request.accept("cluster-communication-protocol", request.origin);
		var sessionName = request.socket._peername.address.replace("::ffff:", "") + ":" + request.socket._peername.port;
		if(! this.wsSessionList[sessionName]){
			this.wsSessionList[sessionName] = {};
			this.wsSessionList[sessionName]["connection"] = connection;
			loggerClass.log("MorganaController", "ノード(クライアント:" + sessionName + ")と接続しました");
			loggerClass.log("MorganaController", "現在の接続ノード数:" + Object.keys(self.wsSessionList).length);
			connection.on("message", function(message){
				self.receiveMessage(message, connection, sessionName);
			});
			connection.on("close", function(reasonCode, description){
				loggerClass.log("MorganaController", "ノード(クライアント:" + sessionName + ")と切断しました");
				self.onWebscoketClose(sessionName);
			});
		}else{
			loggerClass.log("MorganaController", "ノード(クライアント:" + sessionName + ")との2重接続を抑止します");
			connection.close();
		}
	};
	
	MorganaControllerClass.prototype.saveClusterData = function(sessionName){
		if(sessionName){
			var timeStamp = Math.floor(Date.now() / 1000);
			dataStoreClass.getHandle("#system@nodeInfo", function(status, store){
				if(status){
					store.setItem(sessionName, null, timeStamp, function(status){
						if(status){
							loggerClass.log("MorganaController", "ノード情報(" + sessionName + ")を保存しました");
						}else{
							loggerClass.log("MorganaController", "ノード情報(" + sessionName + ")の保存に失敗しました");
						}
					});
				}
			});
		}
	};

	MorganaControllerClass.prototype.removeClusterData = function(sessionName){
		if(sessionName){
			dataStoreClass.getHandle("#system@nodeInfo", function(status, store){
				if(status){
					store.removeItem(sessionName, function(status){
						if(status){
							loggerClass.log("MorganaController", "ノード情報(" + sessionName + ")を削除しました");
						}else{
							loggerClass.log("MorganaController", "ノード情報(" + sessionName + ")の削除に失敗しました");
						}
					});
				}
			});
		}
	};

	MorganaControllerClass.prototype.connectionCycleWorker = function(){
		var self = this;
		for(var standbyNode in this.standbyClusterList){
			setTimeout(function(){
				self.websocketClientStart("ws://" + standbyNode + "/");
			}, 10);
		}
	};

	MorganaControllerClass.prototype.websocketClientStart = function(path){
		var self = this;
		var client = new wsClient();
		client.on("connectFailed", function(error){
			loggerClass.log("MorganaController", "ノード(サーバー)[" + path + "]と接続失敗:" + error);
		});
		client.on("connect", function(connection){
			var sessionName = connection.socket._peername.address.replace("::ffff:", "") + ":" + connection.socket._peername.port;
			if(! self.wsSessionList[sessionName]){
				self.wsSessionList[sessionName] = connection;
				loggerClass.log("MorganaController", "ノード(サーバー:" + sessionName + ")[" + path + "]に接続しました");
				loggerClass.log("MorganaController", "現在の接続ノード数:" + Object.keys(self.wsSessionList).length);
				connection.on("error", function(error){
					connection.close();
				});
				connection.on("close", function(){
					loggerClass.log("MorganaController", "ノード(サーバー:" + sessionName + ")[" + path + "]から切断しました");
					self.onWebscoketClose(sessionName);
				});
				connection.on("message", function(message){
					self.receiveMessage(message, connection, sessionName);
				});
				connection.sendUTF(JSON.stringify({"call": "getNodeInfo", "nodeUID": self.nodeUID}));
				connection.sendUTF(JSON.stringify({
					"call": "broadcastMessage",
					"nodeUID": self.nodeUID,
					"message": {"call": "pleaseClusterInfo", "nodeUID": self.nodeUID}
				}));
			}else{
				loggerClass.log("MorganaController", "ノード(サーバー:" + sessionName + ")[" + path + "]への2重接続を抑止します");
				connection.close();
			}
		});
		client.connect(path, "cluster-communication-protocol");
	};

	MorganaControllerClass.prototype.onWebscoketClose = function(sessionName){
		if(this.wsSessionList[sessionName]){
			delete this.wsSessionList[sessionName];
			loggerClass.log("MorganaController", "現在の接続ノード数:" + Object.keys(this.wsSessionList).length);
		}
		if(this.activeClusterList[sessionName]){
			delete this.activeClusterList[sessionName];
			this.standbyClusterList[sessionName] = true;
			loggerClass.log("MorganaController", "現在のクラスター構成数(-):" + Object.keys(this.activeClusterList).length);
		}
	};

	MorganaControllerClass.prototype.receiveMessage = function(message, connection, sessionName){
		try{
			//message.type == "utf8"
			//message.type == "binary"
			//message.binaryData
			//sendBytes
			var data = JSON.parse(message.utf8Data);
			if(this.receiveCtrlMessageAction[data["call"]]){
				this.receiveCtrlMessageAction[data["call"]].call(this, data, connection, sessionName);
			}else{
				loggerClass.log("MorganaController", "ノード(" + sessionName + ")から未知のコマンド(" + data["call"] + ")"+ "を受信しました");
			}
		}catch(error){
			loggerClass.log("MorganaController", "メッセージ受信処理失敗:" + error);
		}
	};

	MorganaControllerClass.prototype.receiveCtrlMessageAction = {};

	MorganaControllerClass.prototype.receiveCtrlMessageAction.getNodeInfo = function(requestData, connection, sessionName){
		if(requestData["nodeUID"] && requestData["nodeUID"] != this.nodeUID){
			connection.sendUTF(JSON.stringify({"call": "reNodeInfo", "nodeUID": this.nodeUID, "nodeType": "controller"}));
			loggerClass.log("MorganaController", "getNodeInfo:nodeUID(" + requestData["nodeUID"] + ")を受信(" + sessionName + ")");
		}else{
			loggerClass.log("MorganaController", "getNodeInfo:nodeUID(" + this.nodeUID + ")が一致するため切断(" + sessionName + ")");
			connection.close();
		}
	};

	MorganaControllerClass.prototype.receiveCtrlMessageAction.reNodeInfo = function(requestData, connection, sessionName){
		if(requestData["nodeUID"] && requestData["nodeUID"] != this.nodeUID){
			if(requestData["nodeType"] == "storage"){
				if(! this.activeClusterList[sessionName]){
					delete this.standbyClusterList[sessionName];
					this.activeClusterList[sessionName] = sessionName.split(":");
					this.saveClusterData(sessionName);
					loggerClass.log("MorganaController", "reNodeInfo:nodeUID(" + requestData["nodeUID"] + ")をクラスターに追加しました(" + sessionName + ")");
				}else{
					loggerClass.log("MorganaController", "reNodeInfo:nodeUID(" + requestData["nodeUID"] + ")は接続済みクラスターです(" + sessionName + ")");
				}
				loggerClass.log("MorganaController", "現在のクラスター構成数(+):" + Object.keys(this.activeClusterList).length);
			}else{
				loggerClass.log("MorganaController", "reNodeInfo:nodeUID(" + requestData["nodeUID"] + ")はconrollerノードです(" + sessionName + ")");
			}
		}else{
			loggerClass.log("MorganaController", "reNodeInfo:nodeUID(" + this.nodeUID + ")が一致するため切断(" + sessionName + ")");
			connection.close();
		}
	};

	MorganaControllerClass.prototype.receiveCtrlMessageAction.pleaseClusterInfo = function(requestData, connection, sessionName){
		var clusterList = [];
		for(var key in this.activeClusterList){
			clusterList.push(key);
		}
		for(var key in this.standbyClusterList){
			clusterList.push(key);
		}
		connection.sendUTF(JSON.stringify({
			"call": "broadcastMessage",
			"nodeUID": this.nodeUID,
			"message": {"call": "reClusterInfo", "nodeUID": this.nodeUID, "clusterList": clusterList}
		}));
	};

	MorganaControllerClass.prototype.receiveCtrlMessageAction.reClusterInfo = function(requestData, connection, sessionName){
		if(this.nodeUID != requestData["nodeUID"]){
			loggerClass.log("MorganaController", "reClusterInfo:nodeUID(" + requestData["nodeUID"] + ")からクラスター情報が通知されました(" + sessionName + ")");
			for(var i = 0; i < requestData["clusterList"].length; i++){
				var clusterItem = requestData["clusterList"][i];
				if((! this.activeClusterList[clusterItem]) && (! this.standbyClusterList[clusterItem])){
					loggerClass.log("MorganaController", "reClusterInfo:nodeUID(" + requestData["nodeUID"] + ")からの新しいクラスター情報[" + clusterItem + "]を追加(" + sessionName + ")");
					this.standbyClusterList[clusterItem] = true;
				}
			}
		}
		this.connectionCycleWorker();
	};

	MorganaControllerClass.prototype.receiveCtrlMessageAction.heartBeat = function(requestData, connection, sessionName){
		connection.sendUTF(JSON.stringify({"call": "reHeartBeat", "nodeName": this.myNodeName, "nodeUID": this.nodeUID}));
		loggerClass.log("MorganaController", "heartBeat:nodeUID(" + requestData["nodeUID"] + ")からHeartBeatを受信しました(" + sessionName + ")");
	};

	MorganaControllerClass.prototype.receiveCtrlMessageAction.reHeartBeat = function(requestData, connection, sessionName){
		loggerClass.log("MorganaController", "reHeartBeat:nodeUID(" + requestData["nodeUID"] + ")からHeartBeatを受信しました(" + sessionName + ")");
	};

	MorganaControllerClass.prototype.receiveCtrlMessageAction.notify = function(requestData, connection, sessionName){
		loggerClass.log("MorganaController", "notify:nodeUID(" + requestData["nodeUID"] + ")からNotifyを受信しました(" + sessionName + ")");
	};

	MorganaControllerClass.prototype.receiveMessageAction = {};

	MorganaControllerClass.prototype.receiveMessageAction.clusterGetItem = function(request, response, urlPath, cookies){
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		var clusterRequest = new MorganaClusterQueClass(response, "value", requestInfo["responseMode"], {
			"X-requestCall": "clusterGetItem",
			"X-nodeUID": this.nodeUID,
			"X-table": requestInfo["table"],
			"X-key": requestInfo["key"]
		});
		for(var activeNode in this.activeClusterList){
			var host = this.activeClusterList[activeNode][0];
			var port = parseInt(this.activeClusterList[activeNode][1]);
			clusterRequest.addRequest(host, port, "/api/nodeGetItem/?" + urlPath["search"], "GET");
		}
		clusterRequest.end();
	};

	MorganaControllerClass.prototype.receiveMessageAction.clusterExistsItem = function(request, response, urlPath, cookies){
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		var clusterRequest = new MorganaClusterQueClass(response, "bool", requestInfo["responseMode"], {
			"X-requestCall": "clusterExistsItem",
			"X-nodeUID": this.nodeUID,
			"X-table": requestInfo["table"],
			"X-key": requestInfo["key"]
		});
		for(var activeNode in this.activeClusterList){
			var host = this.activeClusterList[activeNode][0];
			var port = parseInt(this.activeClusterList[activeNode][1]);
			clusterRequest.addRequest(host, port, "/api/nodeExistsItem/?" + urlPath["search"], "GET");
		}
		clusterRequest.end();
	};

	MorganaControllerClass.prototype.receiveMessageAction.clusterSetItem = function(request, response, urlPath, cookies){
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		var clusterRequest = new MorganaClusterQueClass(response, "bool", requestInfo["responseMode"], {
			"X-requestCall": "clusterSetItem",
			"X-nodeUID": this.nodeUID,
			"X-table": requestInfo["table"],
			"X-key": requestInfo["key"]
		});
		var sentClusterList = {};
		var limitCount = Math.ceil(this.clusterPolicy["shadowCopyNodes"] * Object.keys(this.activeClusterList).length);
		if(request.method == "GET" || request.method == "POST"){
			var nodes = Object.keys(this.activeClusterList);
			while(Object.keys(sentClusterList).length < limitCount){
				var randomNumber = Math.round(Math.random() * (nodes.length - 1));
				targetNode = nodes[randomNumber];
				if(! sentClusterList[targetNode]){
					sentClusterList[targetNode] = true;
					var host = this.activeClusterList[targetNode][0];
					var port = parseInt(this.activeClusterList[targetNode][1]);
					clusterRequest.addRequest(host, port, "/api/nodeSetItem/?" + urlPath["search"], request.method);
				}
			}
			if(request.method == "POST"){
				request.on("data", function(chunk){
					request.pause();
					clusterRequest.write(chunk);
					request.resume();
				});
				request.on("end", function(){
					clusterRequest.end();
				});
				request.on("error", function(){
					clusterRequest.abort();
				});
			}
			if(request.method == "GET"){
				clusterRequest.end();
			}
		}else{
			response.writeHead(400, {"Content-Type":"text/html", "Access-Control-Allow-Origin": "*"});
			response.end("ERROR::Bad Request");
		}
	};

	MorganaControllerClass.prototype.receiveMessageAction.clusterRemoveItem = function(request, response, urlPath, cookies){
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		var clusterRequest = new MorganaClusterQueClass(response, "bool", requestInfo["responseMode"], {
			"X-requestCall": "clusterRemoveItem",
			"X-nodeUID": this.nodeUID,
			"X-table": requestInfo["table"],
			"X-key": requestInfo["key"]
		});
		for(var activeNode in this.activeClusterList){
			var host = this.activeClusterList[activeNode][0];
			var port = parseInt(this.activeClusterList[activeNode][1]);
			clusterRequest.addRequest(host, port, "/api/nodeRemoveItem/?" + urlPath["search"], "GET");
		}
		clusterRequest.end();
	};

	MorganaControllerClass.prototype.receiveMessageAction.clusterSearchKeys = function(request, response, urlPath, cookies){
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		var clusterRequest = new MorganaClusterQueClass(response, "object", requestInfo["responseMode"], {
			"X-requestCall": "clusterSearchKeys",
			"X-nodeUID": this.nodeUID,
			"X-table": requestInfo["table"],
			"X-key": requestInfo["key"]
		});
		for(var activeNode in this.activeClusterList){
			var host = this.activeClusterList[activeNode][0];
			var port = parseInt(this.activeClusterList[activeNode][1]);
			clusterRequest.addRequest(host, port, "/api/nodeSearchKeys/?" + urlPath["search"], "GET");
		}
		clusterRequest.end();
	};

	MorganaControllerClass.prototype.receiveMessageAction.clusterGetKeys = function(request, response, urlPath, cookies){
		var requestInfo = MorganaFunctionClass.clearOneItemArray(urlPath["get"]);
		var clusterRequest = new MorganaClusterQueClass(response, "object", requestInfo["responseMode"], {
			"X-requestCall": "clusterGetKeys",
			"X-nodeUID": this.nodeUID,
			"X-table": requestInfo["table"],
			"X-offset": requestInfo["offset"],
			"X-limit": requestInfo["limit"]
		});
		for(var activeNode in this.activeClusterList){
			var host = this.activeClusterList[activeNode][0];
			var port = parseInt(this.activeClusterList[activeNode][1]);
			clusterRequest.addRequest(host, port, "/api/nodeGetKeys/?" + urlPath["search"], "GET");
		}
		clusterRequest.end();
	};
	
	return MorganaControllerClass;
})();


(function(){
	try{
		if(typeof window === "undefined" && typeof process !== "undefined" && typeof require !== "undefined"){
			if(require && require.main === module){
				// MainProcess
				(function(){
					loggerClass.log("StartServer",  process.argv[1]);

					loggerClass.log("MorganaStor", "--------------------------------------------------");
					loggerClass.log("MorganaStor", "  - MorganaStor - ");
					loggerClass.log("MorganaStor", "  Network Cluster KV Store");
					loggerClass.log("MorganaStor", "  " + (new Date()).toString());
					loggerClass.log("MorganaStor", "--------------------------------------------------");

					var MorganaStorage = new MorganaStorageClass();
					var MorganaController = new MorganaControllerClass();

					MorganaStorage.start(process.argv[2], __dirname);
					MorganaController.start(process.argv[3], "127.0.0.1:" + process.argv[2], process.argv[4]);

					process.on("SIGINT", function(){
						loggerClass.log("サービスを停止します", "sigintを受信しました");
						MorganaStorage.stop();
						MorganaController.stop();
					});
					process.on("SIGHUP", function(){
						loggerClass.log("サービスを停止します", "sighupを受信しました");
						MorganaStorage.stop();
						MorganaController.stop();
					});
					process.on("SIGTERM", function(){
						loggerClass.log("サービスを停止します", "sigtermを受信しました");
						MorganaStorage.stop();
						MorganaController.stop();
					});
					process.on("exit", function(){
						loggerClass.log("EXIT", "終了");
					});
				})();
			}else if(exports){
				// ChildProcess
				exports.MorganaStorageClass = MorganaStorageClass;
				exports.MorganaControllerClass = MorganaControllerClass;
			}
		}else{
			// BrowserProcess
		}
	}catch(error){
	}
})();