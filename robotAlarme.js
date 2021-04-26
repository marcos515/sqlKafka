const mysql = require('mysql2');
const config = require('./files/config.json')
const kafka = require("./kafka/kafka")
const kafkaConfig = require('./files/kafka.json')
const yargs = require('yargs');
const fs = require('fs');
const TemplateInfoId = require('./files/exportTemplateInfo.json')
require('dotenv').config()

var options = {
    host: process.env.dbHost,
    user: process.env.dbUser,
    database: process.env.dbName,
    password: process.env.dbPasswd,
    waitForConnections: config.waitForConnections,
    connectionLimit: config.connectionLimit,
    queueLimit: config.queueLimit
}

{
    (async () => {
        var tempoDeExecucao = {
            inicio: new Date().getTime(),
            fim: ""
        }
        var args = yargs.argv

        if (args.start && args.end) {

            let start = parseInts(args.start)  //1617182940
            let end = parseInts(args.end) //1617197340

            let p = 60;
            let i = 0;

            var estatistica = {
                dispositivos: 0,
                rows: 0,
                msg: 0
            }

            while ((start + (p * (i + 1))) <= end) {
                let startStep = (start + (p * (i + 1)))
                let endStep = (start + (p * (i + 2)))

                let data = await loop(startStep, endStep, args, args.dispositivoId)

                estatistica.dispositivos = estatistica.dispositivos + data.dispositivos
                estatistica.rows = estatistica.rows + data.rows
                estatistica.msg = estatistica.msg + data.msg

                i++;
            }
            tempoDeExecucao.fim = new Date().getTime()
            console.log(`Tempo de execução: ${(tempoDeExecucao.fim - tempoDeExecucao.inicio) / 60000} minutos`)
            console.log(`${estatistica.rows} linhas de dados recebidos`)
            console.log(`Encontrados dados de ${estatistica.dispositivos} dispositivos`)
            console.log(`Será publicados ${estatistica.msg} mensagens`)
            console.log("Finalizado")

        } else {
            console.log("Sem argumentos")
        }
    })()
}



function parseInts(n) {
    try {
        return parseInt(n)
    } catch (error) {
        return n
    }
}
async function loop(start, end, args, dvid) {

    var data = await queryData(start, end, dvid)
    //console.log(data)
    //node robot --host=192.168.18.48:9092 --topic=data --start=1617706260 --end=1617709860


    var dispositivos = {
        dispositivos: 0,
        dados: []
    }
    for (let index = 0; index < data.length; index++) {
        addArray(dispositivos, data[index])
    }


    for (let index = 0; index < dispositivos.dados.length; index++) {
        //console.log(JSON.stringify(await buildValuesJson(dispositivos.dados[index])))

        let enviou = false
        while (enviou !== true) {
            try {
                //let enviar = 
                //console.log(await buildValuesJson(dispositivos.dados[index]))
                //enviou = true 
                //fs.writeFileSync("./dataTeste.json", JSON.stringify(await buildValuesJson(dispositivos.dados[index])))
                enviou = await kafka.producer(args.host, args.topic, JSON.stringify(await buildValuesJson(dispositivos.dados[index]))).catch((err) => {
                    enviou = false
                })
            } catch (error) {
                console.log("kafka connnect error")
            }

        }
    }

    return {
        dispositivos: dispositivos.dispositivos,
        msg: dispositivos.dados.length,
        rows: data.length
    }
}


async function buildValuesJson(data) {
    var res = { "ts": data.ts, "dispositivoId": data.dispositivoId, "values": [] }
    for (let index = 0; index < data.data.length; index++) {
        // var info = await getNameById(data.data[index].templateInfo_id)
        res.values.push(data.data[index])
       // fs.writeFileSync("./dataTeste.json", JSON.stringify(data.data[index]))
    }
    return res
}

function addArray(array, data) {
    var t = false
    for (let index = 0; index < array.dados.length; index++) {
        if (array.dados[index].dispositivoId == data.dispositivo_id) {
            t = true
        }
        if (array.dados[index].dispositivoId == data.dispositivo_id && array.dados[index].ts == data.dataAtivaRemoto) {

            array.dados[index].data.push(data)
            return 1
            break;
        }
    }
    if (!t) {
        array.dispositivos = array.dispositivos + 1;
    }
    array.dados.push({
        dispositivoId: data.dispositivo_id,
        ts: data.dataAtivaRemoto,
        data: [data]
    })
}

async function queryData(start, end, did) {
    var query = `SELECT * FROM vminew2.OcorrenciaHistoricoNew WHERE dataAtivaRemoto BETWEEN ${start} AND ${end}`

    if (did) {
        query += ` and dispositivo_id = ${did}`
    }
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

async function getNameById(id) {
    let response = []
    response = TemplateInfoId.filter(function (tes) { return tes.id == id; })[0];

    if (response == undefined) {
        var query = `SELECT * FROM vminew2.TemplateInfo where id= ${id};`
        resBd = await sql(options, query)

        return resBd[0]
    } else {
        return response
    }

}


async function sql(options, query) {
    return new Promise((resolve, reject) => {
        const connection = mysql.createConnection(options);
        connection.on("error", (err, msg) => { reject(false) })

        connection.connect();

        connection.query(query, function (error, results, fields) {
            if (error) {
                reject(error)
            };

            connection.end();
            resolve(results)
        });

    })
}