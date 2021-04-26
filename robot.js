const mysql = require('mysql2');
const config = require('./files/config.json')
const kafka = require("./kafka/kafka")
const kafkaConfig = require('./files/kafka.json')
const yargs = require('yargs');
require("dotenv").config()
const fs = require('fs');
const TemplateInfoId = require('./files/exportTemplateInfo.json')

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

        //var name = (await getNameById(1))[0].nome
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
    //console.log(`${data.length} linhas de dados recebidos`)
    var dispositivos = {
        dispositivos: 0,
        dados: []
    }
    for (let index = 0; index < data.length; index++) {
        addArray(dispositivos, data[index])
    }
    /*
    console.log(`Encontrados dados de ${dispositivos.dispositivos} dispositivos`)
    console.log(`Será publicados ${dispositivos.dados.length} mensagens`)
    */
    // var parsedMessages = []
    for (let index = 0; index < dispositivos.dados.length; index++) {
        //parsedMessages.push()
        //kafka.producer(kafkaConfig.host, kafkaConfig.topic, JSON.stringify(await buildValuesJson(dispositivos.dados[index])))
        let enviou = false
        while (enviou !== true) {
            try {
                fs.writeFileSync("./DataTeste.json",  JSON.stringify(await buildValuesJson(dispositivos.dados[index])))
                enviou = await kafka.producer(args.host, args.topic, JSON.stringify(await buildValuesJson(dispositivos.dados[index])))//await sendKafkaMessage(kafkaConfig.topic, JSON.stringify(await buildValuesJson(dispositivos.dados[index])))
            } catch (error) {
                console.log("kafka connnect error")
            }
            //console.log(enviou)
        }
    }
    // console.log(dispositivos.dispositivos)
    return {

        dispositivos: dispositivos.dispositivos,
        msg: dispositivos.dados.length,
        rows: data.length
    }
}


async function buildValuesJson(data) {
    var res = { "ts": data.ts, "dispositivoId": data.dispositivoId, "values": [] }
    for (let index = 0; index < data.data.length; index++) {
        //console.log(data.data[index])
        var info = await getNameById(data.data[index].templateInfo_id)
        //console.log(info)
        res.values.push({
            name: info.apelido,
            value: data.data[index].valor,
            unity: info.unidadeMedida
        })
    }
    //console.log(res)
    return res
}

function addArray(array, data) {
    var t = false
    for (let index = 0; index < array.dados.length; index++) {
        if (array.dados[index].dispositivoId == data.dispositivo_id) {
            t = true
        }
        if (array.dados[index].dispositivoId == data.dispositivo_id && array.dados[index].ts == data.dataRemoto) {
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
        ts: data.dataRemoto,
        data: [data]
    })
}

async function queryData(start, end, did) {
    var query = `SELECT * FROM vminew2.InfoHistorico WHERE dataRemoto BETWEEN ${start} AND ${end}`
    // `SELECT * FROM vminew2.InfoHistorico where dataRemoto > ${start} and dataRemoto <= ${end}`
    if (did) {
        query += ` and dispositivo_id = ${did}`
    }
    return (await sql(options, query + ';'))
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
                reject(false)
            };
            //console.log(`Retornou ${results.length} linhas.`);
            connection.end();
            resolve(results)
        });

    })
}