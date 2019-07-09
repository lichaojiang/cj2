const async = require('async');
const mongoose = require('mongoose');
const jsonSql = require('json-sql');
jsonSql.setDialect('mongodb');
const extend = require('extend');
var sandboxHelper = require('../utils/sandbox');
//private fileds
var modules, library, self, privated = {}, shared = {};

privated.loaded = false;

privated.DOUBLE_DOUBLE_QUOTES = /""/g;
privated.SINGLE_QUOTES = /'/g;
privated.SINGLE_QUOTES_DOUBLED = "''";

//constructor

function Sql(cb, scope) {
    library = scope;
    self = this;
    self.__private = privated;
    setImmediate(cb, null, self);
}

privated.escape = function (what) {
    switch (typeof what) {
        case 'string':
            return "'" + what.replace(
                privated.SINGLE_QUOTES, privated.SINGLE_QUOTES_DOUBLED
            ) + "'";
        case 'object':
            if (what == null) {
                return 'null';
            } else if (Buffer.isBuffer(what)) {
                return "X'" + what.toString('hex') + "'";
            } else {
                return ("'" + JSON.stringify(what).replace(
                    privated.SINGLE_QUOTES, privated.SINGLE_QUOTES_DOUBLED
                ) + "'");
            }
        case 'boolean':
            return what ? '1' : '0'; // 1 => true, 0 => false
        case 'number':
            if (isFinite(what)) return '' + what;
    }
    throw new Error('unsupported data', typeof what);
}

privated.pass = function (obj, dappid) {
    for (const property in obj) {
        if (typeof obj[property] == "object") {
            privated.pass(obj[property], dappid);
        }
        if (property == "table") {
            obj[property] = "dapp_" + dappid + "_" + obj[property];
        }
        if (property == "join" && obj[property].length === undefined) {
            for (var table in obj[property]) {
                var tmp = obj[property][table];
                delete obj[property][table];
                obj[property]["dapp_" + dappid + "_" + table] = tmp;
            }
        }
        if (property == "on" && !obj.alias) {
            for (let firstTable in obj[property]) {
                var secondTable = obj[property][firstTable];
                delete obj[property][firstTable];

                var firstTableRaw = firstTable.split(".");
                firstTable = "dapp_" + dappid + "_" + firstTableRaw[0];

                var secondTableRaw = secondTable.split(".");
                secondTable = "dapp_" + dappid + "_" + secondTableRaw[0];

                obj[property][firstTable] = secondTable;
            }
        }

    }
}
//private methods

privated.query = function (action, config, cb) {
    var sql = null;

    function done(err, data) {
        if (err) {
            err = err.toString();
        }
        cb(err, data);
    }
    if (action != "batch") {
        privated.pass(config, config.dappid);

        var defaultConfig = {
            type: action
        };

        try {
            sql = jsonSql.build(extend({}, config, defaultConfig));
        } catch (e) {
            return done(e.toString());
        }

        if (action = "select") {
            // console.log(sql.query, sql.values)
            //to complete
        } else {
            //to complete
        }


    } else {
        var batchPack = [];
        async.until(
            function () {
                batchPack = config.values.splice(0, 10);
                return batchPack.length == 0;
            }, function (cb) {
                var fileds = Object.keys(config.fileds).map(function (filed) {
                    return (privated.escape(config.fileds[filed]));
                });
                sql = "INSERT INTO" + "dapp_" + config.dappid + "_" + config.table + "(" + fileds.join(",") + ")";
                var rows = [];
                batchPack.forEach(function (value, rowIndex) {
                    var currentRow = batchPack[rowIndex];
                    var fileds = [];
                    for (let i = 0; i < currentRow.length; i++) {
                        fileds.push(privated.escape(currentRow[i]));
                    }
                    rows.push("select" + fileds.join(","));
                });
                sql += (" " + rows.join(" UNION "));
                //to complete
            }, done);
    }
}
//public methods
Sql.prototype.createTables = function (dappid, config, cb) {
    if (!config) {
        return cb("Invalid table format");
    }

    var sqles = [];
    for (let i = 0; i < config.length; i++) {
        config[i].table = "dapp_" + dappid + "_" + config[i].table;
        if (config[i].type == "table") {
            config[i].type = "create";
            if (config[i].foreignKeys) {
                for (let n = 0; n < config[i].foreignKeys.length; n++) {
                    config[i].foreignKeys[n].table = "dapp_" + dappid + "_" + config[i].foreignKeys[n].table;

                }
            }
        } else if (config[i].type == "index") {
            config[i].type = "index";

        } else {
            return setImmediate(cd, "Unknown table type: " + config[i].type);
        }
        var sql = jsonSql.build(config[i]);
        sqles.push(sql.query);
    }

    async.eachSeries(sqles, function (command, cb) {
        //to complete
    }, function (err) {
        setImmediate(cb, err, self);
    });
}

//Drop table functional

sql.prototype.dropTables = function (dappid, config, cb) {
    var tables = [];
    for (let i = 0; i < config.length; i++) {
        config[i].table = "dapp_" + dappid + "_" + config[i].table;
        tables.push({ name: config[i].table.replace(/[^\w_]/gi, ''), type: config[i].type })

    }
    async.eachSeries(tables,function(table,cb){
        if(table.type =="create"){
            //to complete
        }else if(table.type == "index"){
            //to complete
        }else{
            setImmediate(cb);
        }
    },cb)
}

sql.property.sandboxApi = function(call,args,cb){
    sandboxHelper.callMethod(shared,call,args,cb);
}

//events
sql.prototype.onBind = function(scope){
    modules = scope;
}

sql.property.onBloakchainReady = function(){
    privated.loaded = true;
}

//shared
shared.select = function(req,cb){
    var config = extend({},req.body,{dappid:req.dappid});
    privated.query.call(this,"select",config,cb);
}

shared.batch = function(req,cb){
    var config = extend({},req.body,{dappid:req.dappid});
    privated.query.call(this,"batch",config,cb);
}

shared.insert = function(req,cb){
    var config = extend({},req.body,{dappid:req.dappid});
    privated.query.call(this,"insert",config,cb);
}

shared.update = function(req,cb){
    var config = extend({},req.body,{dappid:req.dappid});
    privated.query.call(this,"update",config,cb);
}

shared.remove = function(req,cb){
    var config = extend({},req.body,{dappid:req.dappid});
    privated.query.call(this,"remove",config,cb);
}

module.exports = Sql;