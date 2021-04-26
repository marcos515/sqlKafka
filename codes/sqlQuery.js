const mysql = require('mysql2');
require("dotenv").config()
var options = {
    host: process.env.dbHost,
    user: process.env.dbUser,
    database: process.env.dbName,
    password: process.env.dbPasswd,
    "waitForConnections": true,
    "connectionLimit": 8000000,
    "queueLimit": 90000000
}

module.exports = async (query) => {
    var retornou = false
    while (retornou !== true) {
        try {
            let data = (await sql(options, query + ';'))
            retornou = true
            return data
        } catch (err) {
            retornou = false;
        }
    }
}

async function sql(options, query) {
    return new Promise((resolve, reject) => {
        const connection = mysql.createConnection(options);
        connection.on("error", (err, msg) => { reject(false) })

        connection.connect();

        connection.query(query, function (error, results, fields) {
            if (error) {
                reject(false)
            };
            //console.log(`Retornou ${results.length} linhas.`);
            connection.end();
            resolve(results)
        });

    })
}