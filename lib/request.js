const { Pool } = require("pg");
const config = require("../config/config.json");

const pool = new Pool(config.POSTGRESQL);

module.exports = {

    /**
     * Get a single row of data
     * @param {String} request Query
     * @param {String[]} parameters List of parameters
     */
    GetRow: async function GetRow(request, parameters) {
        const { rows } = await pool.query(request, parameters);
        return rows[0];
    },

    /**
     * Get all rows
     * @param {String} request Query
     * @param {String[]} parameters List of parameters
     */
    GetRows: async function GetRows(request, parameters) {
        const { rows } = await pool.query(request, parameters);
        return rows;
    },

    /**
     * Execute a sql request (UPDATE/CREATE/DELETE)
     * @param {String} request Query
     * @param {String[]} parameters List of parameters
     */
    ExecSql: async function ExecSql(request, parameters) {
        await pool.query(request, parameters);
        return;
    },

};