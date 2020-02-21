var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;

const QUERY = `SELECT l.name as location, l.owner as devOwner, MAX(m.timestamp) as lastData
FROM [iot].[locations] l
LEFT JOIN [iot].[messages] m
ON l.id = m.locationID
GROUP BY l.name, l.owner;`

module.exports = function (context, req) {
    context.log('HTTP Debug Last Message');

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
                    context.log.error("Failed to run debug query")
                    context.log.error(err);
                    context.res = {
                        status: 500,
                        body: "query-failed"
                    };
                    context.done();
                } else {
                    context.log.verbose("Ran debug query");
                }
            });
    
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