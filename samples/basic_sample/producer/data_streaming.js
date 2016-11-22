/**
 * http://usejsdoc.org/
 */
var fs = require('fs');
var csv = require('fast-csv');
var Converter = require("csvtojson").Converter;
var converter = new Converter({});
//const filename = '/home/sarika/fsuResearchShipKAOUnrt_22d9_bc42_c17d.csv'
var filename = "/home/sarika/fsuResearchShipKAOUnrt_22d9_bc42_c17d.csv";

function dataStream(){
	fs.readFile(filename,function(err,data){
		
		if(err){
			throw err;
		}else{
		
			/*
				
							.pipe(csv())
							.on('data',function(data){
								console.log(data.length);
								return data;
							})
							.on('end',function(data){
								console.log(data);
							});
			
			 
			//end_parsed will be emitted once parsing finished 
			converter.on("end_parsed", function (jsonArray) {
			   console.log(jsonArray); //here is your result jsonarray 
			});
			 
			//read from file 
			fs.createReadStream(filename).pipe(csv());*/
			// end_parsed will be emitted once parsing finished
			converter.on("record_parsed", function(jsonObj) {
				console.log(jsonObj)
			});

			// read from file
			fs.createReadStream(filename).pipe(converter);
			
			
		}
		
	})
	
}

module.exports = dataStream;