module.exports = { producer }
/*
async function producer(host, topic, msg) {
    return new Promise(async (resolve, reject) =>{

    const  Kafka  = require('kafkajs').Kafka
 
    const kafka = new Kafka({
      clientId: 'modem',
      brokers: [host]
    })
     
    const producer = kafka.producer()
    
      // Producing
      await producer.connect()
      .then(async (a)=>{
       
      await producer.send({
        topic: topic,
        messages: [
          { value: msg },
        ],
      }).then((a)=>{
        resolve(true)
        producer.disconnect()
      }).catch((a)=>{
        reject(a)
      })

    }).catch((a)=>{
      reject(a)
    })
     
    
   
})
}
module.exports = { producer }

*/
/*

async function producer(host, topic, msg) {
  return new Promise(async (resolve, reject) => {

    const config = {
      noptions: {
        "metadata.broker.list": host,
        "group.id": "sqlRobot",
        "client.id": "sqlRobot1",
        "event_cb": true,
        "compression.codec": "snappy",
        "api.version.request": true,

        "socket.keepalive.enable": true,
        "socket.blocking.max.ms": 100,

        "enable.auto.commit": false,
        "auto.commit.interval.ms": 100,

        "heartbeat.interval.ms": 250,
        "retry.backoff.ms": 250,

        "fetch.min.bytes": 100,
        "fetch.message.max.bytes": 2 * 1024 * 1024,
        "queued.min.messages": 100,

        "fetch.error.backoff.ms": 100,
        "queued.max.messages.kbytes": 50,

        "fetch.wait.max.ms": 1000,
        "queue.buffering.max.ms": 1000,

        "batch.num.messages": 10000
      },
      tconf: {
        "auto.offset.reset": "earliest",
        "request.required.acks": "all"
      },
      batchOptions: {
        "batchSize": 5,
        "commitEveryNBatch": 1,
        "concurrency": 1,
        "commitSync": false,
        "noBatchCommits": false
      }
    };

    const { KafkaStreams } = require("kafka-streams");

    const kafkaStreams = new KafkaStreams(config);

    kafkaStreams.on("error", (error) => {
      reject(error.message);
    });

    const stream = kafkaStreams.getKStream(null);

    stream.to(topic);

    stream.start().then(_ => {

      stream.writeToStream(msg)
       

    }).catch((err)=>{
      console.log(err)
    }).then(()=>{
      resolve(true)
        kafkaStreams.closeAll.bind(kafkaStreams)
     
    })

  })
}*/

async function producer(host, topic, msg) {
  return new Promise(async (resolve, reject) => {

    var kafka = require('kafka-node')
    var HighLevelProducer = kafka.HighLevelProducer;
    var Client = kafka.KafkaClient;
    var conf = {
      kafkaHost: host,
      groupId: 'ExampleTestGroup',
      id: 'consumer1',
      requireAcks: "all",
      ackTimeoutMs: 100,
    }
    var client = new Client(conf);

    var producer = new HighLevelProducer(client);

    producer.on('ready', function () {
      producer.send([{ topic: topic, messages: [msg] }], function (err, data) {
        if (err) {
          producer.close()
          reject(err)
          
        } else {
          producer.close()
          resolve(true);
          
        }
      });
    });

    producer.on('error', function (err) {
      producer.close()
      reject(err)
    });

    
  })
}