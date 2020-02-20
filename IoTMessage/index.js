var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;

const QUERY_LOCATION = `
BEGIN TRAN
    IF EXISTS (SELECT l.id FROM [iot].[locations] l WITH (updlock, rowlock, holdlock) where l.nameID = @nameID) BEGIN
        SELECT l.id as id FROM [iot].[locations] l where l.nameID = @nameID;
    END ELSE BEGIN
        INSERT INTO [iot].[locations] (owner, name, nameID)
        VALUES (@owner, @name, @nameID);

        SELECT SCOPE_IDENTITY() as id;
    END;
COMMIT
`

const QUERY_MESSAGES = `
INSERT INTO [iot].[messages] ([locationID], [timestamp], [readingNumber], [temperatureLPS], [temperatureLSM], [temperatureDHT], [pressureLPS], [humidityDHT], [eco2], [tvoc], [wifiDevices], [wifiBaseStations])
VALUES (@locationID, dateadd(S, @timestamp, '1970-01-01'), @readingNumber, @temperatureLPS, @temperatureLSM, @temperatureDHT, @pressureLPS, @humidityDHT, @eco2, @tvoc, @wifiDevices, @wifiBaseStations);
`

const DATA_FIELDS = {
    "timestamp": { name: "timestamp", fn: parseInt, type: TYPES.Int, min: 1577836800, max: Infinity}, // 2020+
    "count": { name: "readingNumber", fn: parseInt, type: TYPES.Int, min: 20, max: Infinity}, // DISCARD FIRST FEW READINGS FROM EACH SENSOR
    "tempLPS": { name: "temperatureLPS", fn: parseFloat, type: TYPES.Real, min: 5, max: 35 },
    "tempLSM": { name: "temperatureLSM", fn: parseFloat, type: TYPES.Real, min: 5, max: 35 },
    "tempDHT": { name: "temperatureDHT", fn: parseFloat, type: TYPES.Real, min: 5, max: 35 },
    "pressure": { name: "pressureLPS", fn: parseFloat, type: TYPES.Real, min: 850, max: 1100 },
    "humidity": { name: "humidityDHT", fn: parseFloat, type: TYPES.Real, min: 0, max: 100 },
    "eco2": { name: "eco2", fn: parseInt, type: TYPES.SmallInt, min: 400, max: 8192 }, // sensor returns 0 to 8192
    "tvoc": { name: "tvoc", fn: parseInt, type: TYPES.SmallInt, min: 0, max: 1200 }, // sensor returns from 0 to 1187
    "devs": { name: "wifiDevices", fn: parseInt, type: TYPES.SmallInt, min: 0, max: 1000 },
    "bss": { name: "wifiBaseStations", fn: parseInt, type: TYPES.SmallInt, min: 0, max: 1000 }
};

module.exports = function (context, IoTHubMessages) {
    function getLocationID(owner, name, nameID, callback) {
        var request = new Request(QUERY_LOCATION, (err) => {
            if (err) {
                context.log.error("Failed to run location query")
                context.log.error(err);
                context.done();
            } else {
                context.log.verbose("Ran location query");
            }
        });

        request.addParameter('owner', TYPES.VarChar, owner);
        request.addParameter('name', TYPES.VarChar, name);
        request.addParameter('nameID', TYPES.VarChar, nameID);

        let locID = -1;

        request.on('row', (columns) => {
            locID = columns[0].value;
        });

        request.on('requestCompleted', function() {
            callback(locID);
        });

        connection.execSql(request);
    }

    function processRow(rows, fields, locID) {
        if (rows.length == 0) {
            // done with message
            nextMessage();
            return;
        }
        const row = rows.shift();
        var timestamp = -1;

        var request = new Request(QUERY_MESSAGES, (err) => {
            if (err) {
                context.log.error("Failed to run insert message")
                context.log.error(err);
                context.done();
            } else {
                context.log.info("Ran message query for timestamp " + timestamp)
                inserted += 1;
            }
        });
        request.addParameter('locationID', TYPES.Int, locID);


        var error = false;
        row.split(",").map(x => x.trim()).forEach((item, i) => {
            const field = DATA_FIELDS[fields[i]];
            if (field.name == "timestamp") timestamp = item;
            const value = field.fn(item);
            if (value > field.max || value < field.min) {
                error = true;
                context.err.log("Invalid entry - " + field.name + ": " + value);
            } else {
                request.addParameter(field.name, field.type, value)
            }
        });

        request.on('requestCompleted', function() {
            processRow(rows, fields, locID);
        });

        if (error) {
            context.err.log("Skipping entry")
            processRow(rows, fields, locID);
        } else {
            connection.execSql(request);
        }
    }

    function processMessage(message, i) {
        const properties = context.bindingData.propertiesArray[i];
        const owner = properties.owner;
        const name = properties.name;
        const nameID = name.replace(/[ ]/gi, '-').replace(/[^0-9a-z-]/gi, '').toLowerCase();
        context.log.verbose("Message from " + nameID);

        var rows = message.data.trim().split("\n");
        var fields = rows.shift().split(",").map(x => x.trim());

        // hack until wifi data sent to cloud
        if (!fields.includes("devs")) {
            fields.push("devs")
            rows = rows.map(x => x + ",0");
        }
        if (!fields.includes("bss")) {
            fields.push("bss")
            rows = rows.map(x => x + ",0");
        }

        getLocationID(owner, name, nameID, (locID) => {
            context.log.info("nameID: " + nameID + " locID:" + locID);
            processRow(rows, fields, locID);
        })
    }

    function nextMessage() {
        if (IoTHubMessages.length == 0) {
            context.log.info("Done! inserted " + inserted + " rows")
            context.done();
            return
        }

        processMessage(IoTHubMessages.shift(), messageI);
        messageI += 1;
    }

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
    var messageI = 0;
    var inserted = 0;

    connection.on('connect', function (err) {
        if (err) {
            context.log.error("Failed to connect to DB!")
            context.log.error(err);
            context.done();
        } else {
            context.log.verbose("Connected to DB")
            nextMessage();
        }
    });
};