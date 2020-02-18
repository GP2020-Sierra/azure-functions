module.exports = function (context, IoTHubMessages) {
    IoTHubMessages.forEach((message, i) => {
        const properties = context.bindingData.propertiesArray[i];
        const owner = properties.owner;
        const name = properties.name;

        // context.log(`Data: ${message.data}`);
        // context.log(`Owner: ${owner}`);
        // context.log(`Name: ${name}`);

        let rows = message.data.trim().split("\n");
        let fields = rows.shift().split(",").map(x => x.trim());
        let processed = [];
        
        rows.forEach(row => {
            let o = {};
            row.split(",").forEach((item, i) => o[fields[i]] = item.trim());
            processed.push(o);
        });

        context.log(JSON.stringify({properties: {owner: owner, name: name}, data: processed}));
    });

    context.done();
};