var Connection = require('tedious').Connection;

module.exports = {
    dbConfig: {
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
    },
    dbFunction (context, callback) {
        var connection = new Connection(this.dbConfig);
    
        connection.on('connect', function (err) {
            if (err) {
                context.log.error("Failed to connect to DB!")
                context.log.error(err);
                context.res = {
                    status: 500,
                    body: "db-failed"
                };
                context.done(err);
            } else {
                context.log.verbose("Connected to DB")

                const callbacks = {
                    _closeDB() {
                        // try to close db
                        context.log.verbose("Trying to close DB")
                        try {
                            connection.close();
                        } catch (err) {
                            context.log.verbose("Failed to close DB", err)
                        }
                    },
                    done () {
                        callbacks._closeDB();
                        context.done();
                    },
                    error(err) {
                        context.log.error("Failed to run query");
                        context.log.error(err);
                        callbacks._closeDB();
                        context.done(err);
                    },
                    ifError(err) {
                        if (err) { callbacks.error(err) }
                    },
                    errorHttp(err) {
                        context.res = {
                            status: 500,
                            body: "query-failed"
                        };
                        callbacks.error(err);
                    },
                    ifErrorHttp(err) {
                        if (err) { callbacks.errorHttp(err) }
                    },
                };

                try {
                    callback(connection, callbacks);
                } catch (err) {
                    context.done(err);
                }
            }
        })
    }
}