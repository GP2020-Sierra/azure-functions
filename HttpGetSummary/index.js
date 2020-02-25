var Request = require('tedious').Request;
var DBFunctions = require("../src/DBFunctions");

const QUERY = `
With JoinedData AS (
    SELECT l.name as name, l.nameID as locationID, dateadd(mi, datediff(mi,0, m.timestamp), 0) as timestamp, AVG(m.temperatureDHT) as temperature, AVG(m.pressureLPS) as pressure, AVG(m.humidityDHT) as humidity, AVG(m.eco2) as co2, AVG(m.wifiDevices) as devices, COUNT(m.timestamp) as '_count'
    FROM [iot].[messages] m
    INNER JOIN [iot].[locations] l
    ON m.locationID = l.id
    WHERE l.shown = 1
    GROUP BY l.name, l.nameID, datediff(mi,0, m.timestamp)
), WithIdx AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY name ASC, timestamp DESC) as _idx
    FROM JoinedData
)

SELECT *
FROM WithIdx
WHERE _idx < 16`

module.exports = function (context, req) {
    context.log('HTTP Get Summary');

    DBFunctions.dbFunction(context, (connection, callbacks) => {
        var request = new Request(QUERY, callbacks.ifErrorHttp);
        
        let locations = [];
        request.on('row', (columns) => {
            function processColumn(c, obj) {
                obj[c.metadata.colName] = c.value;
            }

            var locDetails = {data: []}
            processColumn(columns.shift(), locDetails);
            processColumn(columns.shift(), locDetails);
            if (locations.length > 0 && locations[locations.length - 1].locationID === locDetails.locationID) {
                locDetails = locations[locations.length - 1];
            } else {
                locations.push(locDetails);
            }

            var reading = {}
            columns.forEach(c => processColumn(c, reading));
            reading._idx = parseInt(reading._idx);
            locDetails.data.push(reading);
        });

        request.on('requestCompleted', function() {
            context.res = {
                body: JSON.stringify(locations)
            }
            callbacks.done();
        });

        connection.execSql(request);
    });
};