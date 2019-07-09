const mongoose = require('mongoose');

var Schema = mongoose.Schema;
//connect
const db = mongoose.createConnection('mongodb://127.0.0.1/Ebook',{useMongoClient:true});

//design connection schema
var testSchema = new Schema({

});

var Test = mongoose.model('Test',testSchema);

