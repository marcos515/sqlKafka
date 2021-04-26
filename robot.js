const kafka = require("./codes/kafka");
const yargs = require('yargs');
const queryData = require("./codes/queryData");
const subFunctions = require("./codes/subFunctions");
const parseStandartFormat = require("./codes/parseStandartFormat");
require("dotenv").config();

const fs = require('fs');


{
    (async () => {
        var tempoDeExecucao = {
            inicio: new Date().getTime(),
            fim: ""
        }
        var args = yargs.argv

        if (args.start && args.end) {

            let start = subFunctions.parseInts(args.start)  //1617182940
            let end = subFunctions.parseInts(args.end) //1617197340

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

                var data = await queryData(startStep, endStep, args.dispositivoId)

                var dispositivos = {
                    dispositivos: 0,
                    dados: []
                }
                for (let index = 0; index < data.length; index++) {
                    agrupar(dispositivos, data[index])
                }

                for (let index = 0; index < dispositivos.dados.length; index++) {

                    let enviou = false
                    while (enviou !== true) {
                        try {

                            enviou = await kafka.producer(args.host, args.topic, JSON.stringify(await parseStandartFormat(dispositivos.dados[index])))
                        } catch (error) {
                            console.log("kafka connnect error")
                        }

                    }
                }

                estatistica.dispositivos += dispositivos.dispositivos
                estatistica.rows += data.length
                estatistica.msg += dispositivos.dados.length

                i++;
            }

            tempoDeExecucao.fim = new Date().getTime()
            console.log(`Tempo de execução: ${(tempoDeExecucao.fim - tempoDeExecucao.inicio) / 60000} minutos`)
            console.log(`${estatistica.rows} linhas de dados recebidos`)
            console.log(`Encontrados dados de ${estatistica.dispositivos} dispositivos`)
            console.log(`Enviados ${estatistica.msg} mensagens ao Kafka.`)
            console.log("Finalizado")
        } else {
            console.log("Sem argumentos")
        }
    })()
}
function agrupar(array, data) {
    var t = false
    for (let index = 0; index < array.dados.length; index++) {
        if (array.dados[index].dispositivoId == data.dispositivo_id) {
            t = true
        }
        if (array.dados[index].dispositivoId == data.dispositivo_id && array.dados[index].ts == data.dataRemoto) {
            array.dados[index].data.push(data)
            return 1
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