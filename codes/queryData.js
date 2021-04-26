const sql = require("./sqlQuery")

async function queryData(start, end, did) {
    var query = `SELECT * FROM vminew2.InfoHistorico WHERE dataRemoto BETWEEN ${start} AND ${end}`

    if (did) {
        query += ` and dispositivo_id = ${did}`
    }
    return (await sql(query + ';'))
}

module.exports =  queryData;