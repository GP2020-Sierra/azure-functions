var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;

const QUERY = `
SELECT TOP 1000 dateadd(mi, datediff(mi,0, timestamp) / @perMinutes * @perMinutes, 0) as timestamp, AVG(temperatureDHT) as temperature, AVG(pressureLPS) as pressure, AVG(humidityDHT) as humidity, AVG(eco2) as eco2, AVG(wifiDevices) as wifiDevices, COUNT(timestamp) as '_count'
FROM [iot].[messages] m
INNER JOIN [iot].[locations] l
ON m.locationID = l.id and l.nameID = @location
GROUP BY dateadd(mi, datediff(mi,0, timestamp) / @perMinutes * @perMinutes, 0)
ORDER BY timestamp DESC`

module.exports = function (context, req) {
    context.log('HTTP Get Data');

    // connect to DB
    const config = {
        server: process.env["DATABASE_SERVER"],
        authentication: {
            type: "default",
            options: {
                userName: process.env["DATABASE_USERNAME"],
                password: process.env["DATABASE_PASSWORD"],
            }
        },
        options: {
            database: process.env["DATABASE_NAME"],
            encrypt: true
        }
    };

    var connection = new Connection(config);

    connection.on('connect', function (err) {
        if (err) {
            context.log.error("Failed to connect to DB!")
            context.log.error(err);
            context.res = {
                status: 500,
                body: "db-failed"
            };
            context.done();
        } else {
            context.log.verbose("Connected to DB")
            
            var request = new Request(QUERY, (err) => {
                if (err) {
                    context.log.error("Failed to run data query")
                    context.log.error(err);
                    context.res = {
                        status: 500,
                        body: "query-failed"
                    };
                    context.done();
                } else {
                    context.log.verbose("Ran data query");
                }
            });
            request.addParameter("location", TYPES.NVarChar, context.bindingData.locationID);
            request.addParameter("perMinutes", TYPES.Int, req.query.granularity || (req.body && req.body.granularity) || 5)
            // TODO add start date & end date or length of time fields
            // TODO allow limiting - e.g. 1 record for twitter bot
    
            let result = [];
    
            request.on('row', (columns) => {
                var o = {}
                columns.forEach(x => o[x.metadata.colName] = x.value);
                result.push(o);
            });
    
            request.on('requestCompleted', function() {
                context.res = {
                    body: JSON.stringify(result)
                }
                context.done();
            });
    
            connection.execSql(request);
        }
    });
};