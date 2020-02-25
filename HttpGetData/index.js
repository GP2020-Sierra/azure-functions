var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
var DBFunctions = require("../src/DBFunctions");

const QUERY = `
SELECT TOP 1000 dateadd(mi, datediff(mi, 0, timestamp) / @perMinutes * @perMinutes, 0) as timestamp, AVG(temperatureDHT) as temperature, AVG(pressureLPS) as pressure, AVG(humidityDHT) as humidity, AVG(eco2) as co2, AVG(wifiDevices) as devices, COUNT(timestamp) as '_count'
FROM [iot].[messages] m
INNER JOIN [iot].[locations] l
ON m.locationID = l.id and l.nameID = @location
WHERE datediff(mi, '1970-01-01', timestamp) >= ((@since / 60 / @perMinutes) - 1) * @perMinutes
GROUP BY datediff(mi,0, timestamp) / @perMinutes
ORDER BY timestamp DESC`

module.exports = function (context, req) {
    context.log('HTTP Get Data');

    DBFunctions.dbFunction(context, (connection, callbacks) => {
        var request = new Request(QUERY, callbacks.ifErrorHttp);
        request.addParameter("location", TYPES.NVarChar, context.bindingData.locationID);
        request.addParameter("perMinutes", TYPES.Int, req.query.granularity || (req.body && req.body.granularity) || 5)
        request.addParameter("since", TYPES.Int, req.query.since || (req.body && req.body.since) || 1)

        let result = [];

        request.on('row', (columns) => {
            var o = {}
            columns.forEach(x => o[x.metadata.colName] = x.value);
            result.push(o);
        });

        request.on('requestCompleted', function() {
            if (result.length > 0) {
                context.res = {
                    body: JSON.stringify(result)
                }
            } else {
                context.res = {
                    status: 404,
                    body: "Location not found"
                }
            }
            callbacks.done();
        });

        connection.execSql(request);
    });
};