
var kafka = require('kafka-node')
/**
 * @param {string} host
 * @param {string} topic
 */
function consumer(host, topic) {
    Consumer = kafka.Consumer,
        client = new kafka.KafkaClient({ kafkaHost: host }),
        consumer = new Consumer(
            client,
            [
                { topic: topic, partition: 0 }
            ],
            {

                autoCommit: true,
                fromOffset: false,

                encoding: 'utf8',
                keyEncoding: 'utf8'
            }
        );
    return consumer
}
/**
 * @param {string} host
 * @param {string} topic
 * @param {string} msg
 */
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
module.exports = {
    consumer,
    producer
}

