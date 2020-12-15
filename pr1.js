
const axios = require("axios");
const { producer } = require("./kafka");

(async () => {
    try {
        await getApiData();
        setInterval(getApiData, 30*60*3600);
    } catch (err) {
        console.error(err);
    }
})();

/**
 * Recover API data every 30 minutes and store them
 */
async function getApiData() {
    try {
        const { data } = await axios.get("https://api.covid19api.com/summary");

        await producer.connect();
        await producer.send({
            topic: "topic1",
            messages: [
                { value: JSON.stringify(data) },
            ],
        });
        
        await producer.disconnect();
        console.info("Data recovered from API");
    } catch (err) {
        console.error(err);
    }
}