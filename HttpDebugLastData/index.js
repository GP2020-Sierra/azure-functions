var Request = require('tedious').Request;
var DBFunctions = require("../src/DBFunctions");

const QUERY = `SELECT l.name as location, l.owner as devOwner, MAX(m.timestamp) as lastData
FROM [iot].[locations] l
LEFT JOIN [iot].[messages] m
ON l.id = m.locationID
GROUP BY l.name, l.owner;`

module.exports = function (context, req) {
    context.log('HTTP Debug Last Message');

    DBFunctions.dbFunction(context, (connection, callbacks) => {
        var request = new Request(QUERY, callbacks.ifErrorHttp);

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
            callbacks.done();
        });

        connection.execSql(request);
    });
};