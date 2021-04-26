const getNameById = require("./getNameById")
async function parseStandartFormat(data) {
    var res = { "ts": data.ts, "dispositivoId": data.dispositivoId, "values": [] }
    for (let index = 0; index < data.data.length; index++) {

        var info = await getNameById(data.data[index].templateInfo_id)
        res.values.push({
            name: info.apelido,
            value: data.data[index].valor,
            unity: info.unidadeMedida
        })
    }

    return res
}

module.exports = parseStandartFormat