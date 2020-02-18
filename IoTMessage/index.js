const fieldsFunctions = {
    "owner": (x => x),
    "name": (x => x),
    "timestamp": parseInt,
    "count": parseInt,
    "tempLPS": parseFloat,
    "tempLSM": parseFloat,
    "tempDHT": parseFloat,
    "pressure": parseFloat,
    "humidity": parseFloat,
    "eco2": parseInt,
    "tvoc": parseInt,
    "devs": parseInt,
    "bss": parseInt
};

module.exports = (context, IoTHubMessages) => {
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
                const field = fields[i];
                o[field] = fieldsFunctions[field](item);
            });
            dbDocuments.push(o);
        });
    });

    context.bindings.documents = JSON.stringify(dbDocuments);

    context.done();
};