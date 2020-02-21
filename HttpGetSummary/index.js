var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;

const QUERY = `
With JoinedData AS (
    SELECT l.name as name, l.nameID as locationID, dateadd(mi, datediff(mi,0, m.timestamp), 0) as timestamp, AVG(m.temperatureDHT) as temperature, AVG(m.pressureLPS) as pressure, AVG(m.humidityDHT) as humidity, AVG(m.eco2) as eco2, AVG(m.wifiDevices) as wifiDevices, COUNT(m.timestamp) as '_count'
    FROM [iot].[messages] m
    INNER JOIN [iot].[locations] l
    ON m.locationID = l.id
    GROUP BY l.name, l.nameID, dateadd(mi, datediff(mi,0, m.timestamp), 0)
), WithIdx AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY locationName ORDER BY locationName ASC, timestamp DESC) as _idx
    FROM JoinedData
)

SELECT *
FROM WithIdx
WHERE _idx < 16`

module.exports = function (context, req) {
    context.log('HTTP Get Summary');

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
                    context.log.error("Failed to run summary query")
                    context.log.error(err);
                    context.res = {
                        status: 500,
                        body: "query-failed"
                    };
                    context.done();
                } else {
                    context.log.verbose("Ran summary query");
                }
            });
            
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
                context.done();
            });
    
            connection.execSql(request);
        }
    });
};