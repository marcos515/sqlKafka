const TemplateInfoId = require('../files/exportTemplateInfo.json')

const sql = require("./sqlQuery")

async function getNameById(id) {
    let response = []
    response = TemplateInfoId.filter(function (tes) { return tes.id == id; })[0];

    if (response == undefined) {
        var query = `SELECT * FROM vminew2.TemplateInfo where id= ${id};`
        resBd = await sql(query)

        return resBd[0]
    } else {
        return response
    }

}

module.exports = getNameById