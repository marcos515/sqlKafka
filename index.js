const yargs = require('yargs');
const fs = require("fs");
const exec = require('exec');
const restify = require('restify')
const mysql = require('mysql2');
const config = require('./files/config.json')
const axios = require("axios")
const botLog = require("./files/sendLog")



const server = restify.createServer({
  name: 'SqlKafka',
  version: '1.0.0',
  formatters: {
    'text/html': function (req, res, body) {
      return body;
    }
  }
});

var args = yargs.argv
var globalLog = ""


var options = {
  host: process.env.dbHost,
  user: process.env.dbUser,
  database: process.env.dbName,
  password: process.env.dbPasswd,
  waitForConnections: config.waitForConnections,
  connectionLimit: config.connectionLimit,
  queueLimit: config.queueLimit
}

function atualizaTemplateInfoFile() {
  return new Promise(async (resolve, reject) => {
    var query = `SELECT * FROM vminew2.TemplateInfo;`
    try {
      var data = await sql(options, query)
      resolve(data)
    } catch (error) {
      reject(error)
    }
  })
}

atualizaTemplateInfoFile().then((value) => {
  fs.writeFileSync('./files/exportTemplateInfo.json', JSON.stringify(value))
  wLog(`Sucesso ao atualizar os dados da tabela Template info`);
 
}).then((value) => {
  server.listen(args.port || 3000, async function () {
    wLog(`Servidor SQL Info Histórico -> Kafka online em: "http://${await getIPAddress()}:${args.port || 3000}"`);
  });
}).catch((err) => {
  console.log("Houve um erro ao atualizar os dados da tabela Template info", err)
})


server.on('NotFound', function (req, res, err, cb) {
  res.send(404, fs.readFileSync('./files/sites/error.html', 'utf8'))
});

server.use(restify.plugins.bodyParser({ mapParams: true }));


server.get('/', (req, res) => {
  res.header("Content-Type", "text/html")
  res.header("Access-Control-Allow-Origin", "*")
  res.send(200, fs.readFileSync('./files/sites/index.html', 'utf8'))
});

server.post('/log', (req, res) => {
  res.header("Access-Control-Allow-Origin", "*")
  res.send(globalLog)
})

server.post('/command', (req, res) => {
  res.header("Access-Control-Allow-Origin", "*")
  wLog(jsonString(req.body))
  ValidCommand(jsonJson(req.body))
  res.send("Received command, processing...")
})

server.post('/clearLog', (req, res) => {
  res.header("Access-Control-Allow-Origin", "*")
  globalLog = ""
  res.send("Received command, processing...")
})

server.get('*/file/*',
  restify.plugins.serveStaticFiles('./')
);

function ValidCommand(json) {
  if (validValue(json.start) && validValue(json.end) && validValue(json.threads) && validValue(json.kafkaHost) && validValue(json.kafkaTopic)) {
    if (json.dispositivoId !== undefined) {
      if (validValue(json.dispositivoId)) {
        //wLog("valido")
        runthreads(json.start, json.end, json.threads, json.kafkaHost, json.kafkaTopic, json.dispositivoId)
      } else {
        wLog("Dispositivo id invalido")
      }
    } else {
      //wLog("valido")
      runthreads(json.start, json.end, json.threads, json.kafkaHost, json.kafkaTopic)
    }

  } else {
    wLog("Comando invalido")
  }
}
function validValue(value) {
  if (value == "" || value == null || value == 'null' || value == undefined || value == "undefined") {
    return false
  } else {
    return true
  }
}

function runthreads(startp, endp, robotsp, kafkaHost, kafkaTopic, targetId) {
  let start = (startp * 1000)
  let end = (endp * 1000)
  let numRobots = robotsp
  let tempoPs = parseInt(end - start)
  let tempoR = (tempoPs / numRobots)
  let robotsJob = []

  wLog(`Tempo a ser processado: ${tempoPs / 60000} minutos`)
  wLog(`Tempo para cada robo: ${tempoR / 60000} minutos`)

  for (let index = 0; index < numRobots; index++) {
    robotsJob[index] = {
      start: (start + (tempoR * index)) / 1000,
      end: (start + (tempoR * (index + 1))) / 1000
    }

  }

  wLog(JSON.stringify(robotsJob))
  wLog("Iniciando execução... Aguarde.")
  if (targetId) {
    for (let index = 0; index < robotsJob.length; index++) {
      exec(["node", "robot", `--host=${kafkaHost}`, `--topic=${kafkaTopic}`, `--start=${robotsJob[index].start}`, `--end=${robotsJob[index].end}`, `--dispositivoId=${targetId}`], log.bind({ index: index + 1 }));

    }
  } else {
    for (let index = 0; index < robotsJob.length; index++) {
      exec(["node", "robot", `--host=${kafkaHost}`, `--topic=${kafkaTopic}`, `--start=${robotsJob[index].start}`, `--end=${robotsJob[index].end}`], log.bind({ index: index + 1 }));

    }
  }

  //node robot --host=192.168.18.48:9092 --topic=data --start=1617706260 --end=1617709860
}

function log(err, out, code) {
  fs.writeFileSync("./files/errorLog.txt", fs.readFileSync("./files/errorLog.txt", 'utf-8') + "\n" + err)
  wLog(`Robot ${this.index} error: [ O Log de erro foi escrito no arquivo]`)
  wLog(`Robot ${this.index} out: [${out}]`)
  wLog(`Robot ${this.index} exit code: [${code}]`)

}

async function getIPAddress() {
  try {
    let x = (await axios.get("https://ifconfig.me")).data
    if(x == undefined){
      throw new Error('teste2 deu erro');
    }
    return x
  } catch (error) {

    var interfaces = require('os').networkInterfaces();
    for (var devName in interfaces) {
      var iface = interfaces[devName];

      for (var i = 0; i < iface.length; i++) {
        var alias = iface[i];
        if (alias.family === 'IPv4' && alias.address !== '127.0.0.1' && !alias.internal)
          return alias.address;
      }
    }

  }
  return "0.0.0.0"
}

function wLog(msg) {
  console.log(msg)
  botLog("** SQL Info Histórico --> Kafka **\n"+msg)
  globalLog += msg + "\n";
}

function jsonString(json) {
  try {
    return JSON.stringify(json)
  } catch (err) {
    return json
  }
}

function jsonJson(json) {
  try {
    return JSON.parse(json)
  } catch (err) {
    return json
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