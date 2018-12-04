export default class Producer {

  constructor({ groupPrefix, endpoint, topics, logger, kafka }) {
    this.kafka = kafka;
    this.log = logger;
    this.groupPrefix = groupPrefix;
    this.endpoint = endpoint;
    this.topics = topics;
  }

  startProducer() {
    const ExchangePartitioner = (partitions, key) => {
      if (!key) return 0;
      const exchanges = [0];
      let exchangeIndex = exchanges.indexOf(key);
      exchangeIndex = exchangeIndex < 0 ? 0 : exchangeIndex;
      const index = exchangeIndex % partitions.length;
      return partitions[index];
    };
    const partitionerType = ExchangePartitioner ? 4 : 2;
    const producer = new this.kafka.Producer(new this.kafka.KafkaClient({ kafkaHost: this.endpoint }), { partitionerType }, ExchangePartitioner);
    const log = this.log;
    producer.on('ready', function () {
      log.debug('Producer ready');
    });
    
    producer.on('error', function (err) {
      log.error('Kafka producer error: <%o>', err);
    });
    this.log.debug('Producer created');
    this.producer = producer;
  }

  sendMessage(topic, key, message) {
    let kafkaMessage;
    if (key) {
      kafkaMessage = new this.kafka.KeyedMessage(key, JSON.stringify(message));
    }
    else {
      kafkaMessage = JSON.stringify(message);
    }
    const log = this.log;
    this.producer.send([
      { topic: topic, partition: 0, messages: [kafkaMessage] }
    ], function (err, result) {
      log.debug('Sending: <%o>', err || result);
    });
  }
}