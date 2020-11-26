const { Pool } = require("pg");
const config = require("./config/config.json");
const { producer } = require("./kafka");

const pool = new Pool(config.POSTGRESQL);

pool.query("SELECT NOW()", (err, res) => {
    if(err) console.error(err);
    else console.log(res);
    pool.end();
});


(async () => {
    try {
        await producer.connect();
        await producer.send({
            topic: "topic1",
            messages: [
                { value: 'Hello World !' },
            ],
        });
        
        await producer.disconnect();
    } catch (err) {
        console.error(err);
    }
})();