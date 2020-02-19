const fieldsFunctions = {
    "owner": {name: "owner", fn: x => x},
    "name": {name: "name", fn: x => x},
    "timestamp": {name: "timestamp", fn: parseInt},
    "count": {name: "readingNumber", fn: parseInt},
    "tempLPS": {name: "temperatureLPS", fn: parseFloat},
    "tempLSM": {name: "temperatureLSM", fn: parseFloat},
    "tempDHT": {name: "temperatureDHT", fn: parseFloat},
    "pressure": {name: "pressureLPS", fn: parseFloat},
    "humidity": {name: "humidityDHT", fn: parseFloat},
    "eco2": {name: "eco2", fn: parseInt},
    "tvoc": {name: "tvoc", fn: parseInt},
    "devs": {name: "wifiDevices", fn: parseInt},
    "bss": {name: "wifiBaseStations", fn: parseInt}
};

module.exports = async (context, IoTHubMessages) => {
    let dbDocuments = []

    IoTHubMessages.forEach((message, i) => {
        const properties = context.bindingData.propertiesArray[i];
        const owner = properties.owner;
        const name = properties.name;

        // context.log(`Data: ${message.data}`);
        // context.log(`Owner: ${owner}`);
        // context.log(`Name: ${name}`);

        let rows = message.data.trim().split("\n");
        let fields = rows.shift().split(",").map(x => x.trim());
        
        rows.forEach(row => {
            let o = {owner: owner, name: name};
            row.split(",").map(x => x.trim()).forEach((item, i) => {
                const field = fieldsFunctions[fields[i]];
                o[field.name] = field.fn(item);
            });
            dbDocuments.push(o);
        });
    });

    return {
        documents: dbDocuments
    }
};