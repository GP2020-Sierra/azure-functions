var Request = require('tedious').Request;
var DBFunctions = require("../src/DBFunctions");

const QUERY = `SELECT name, nameID as locationID
FROM [iot].[locations]
WHERE shown = 1;`

module.exports = function (context, req) {
    context.log('HTTP Location');

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
    })
};