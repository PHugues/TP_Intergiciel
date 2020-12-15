const { consumer } = require("./kafka");
const request = require("./lib/request");

(async () => {
    try {
        await getTopicData();
    } catch (err) {
        console.error(err);
    }
})();


/**
 * Get data from the kafka topic
 */
async function getTopicData() {
    try {
        await consumer.connect();
        await consumer.subscribe({
            topic: "topic1",
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    console.info("New message received");
                    const { Global, Countries } = JSON.parse(message.value.toString());
                    // Check if world table isn't empty
                    let world = await request.GetRow("Select * From world");
                    if(!world) {
                        await request.ExecSql('Insert into world Values($1, $2, $3, $4, $5, $6, current_date)', [Global.NewConfirmed, Global.TotalConfirmed, Global.NewDeaths, Global.TotalDeaths, Global.NewRecovered, Global.TotalRecovered]);
                    } else {
                        await request.ExecSql('Update world Set "NewConfirmed" = $1, "TotalConfirmed" = $2, "NewDeaths" = $3, "TotalDeaths" = $4, "NewRecovered" = $5, "TotalRecovered" = $6, "Datemaj" = current_date', [Global.NewConfirmed, Global.TotalConfirmed, Global.NewDeaths, Global.TotalDeaths, Global.NewRecovered, Global.TotalRecovered]);
                    }
                    for(let country of Countries) {
                        // Check if country tuple already exists
                        let c = await request.GetRow('Select * From countries Where "CountryCode"=$1', [country.CountryCode]);
                        if(!c) {
                            await request.ExecSql('Insert into countries Values($1, $2, $3, $4, $5, $6, $7, $8, $9, current_date)', [country.Country, country.CountryCode, country.Slug, country.NewConfirmed, country.TotalConfirmed, country.NewDeaths, country.TotalDeaths, country.NewRecovered, country.TotalRecovered]);
                        } else {
                            await request.ExecSql('Update countries Set "NewConfirmed" = $1, "TotalConfirmed" = $2, "NewDeaths" = $3, "TotalDeaths" = $4, "NewRecovered" = $5, "TotalRecovered" = $6, "Datemaj" = current_date Where "CountryCode" = $7', [country.NewConfirmed, country.TotalConfirmed, country.NewDeaths, country.TotalDeaths, country.NewRecovered, country.TotalRecovered, country.CountryCode]);
                        }
                    }
                    console.info("Updated countries");
                } catch (err) {
                    console.error(err);
                }
            },
        });
    } catch (err) {
        console.error(err);
    }
}