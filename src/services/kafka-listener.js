const MAX_RETRIES = 3;
const SESSION_TIMEOUT = 15000;

export default class Consumer {

  constructor({ groupPrefix, endpoint, topics, logger, kafka }) {
    this.kafka = kafka;
    this.log = logger;
    this.groupPrefix = groupPrefix;
    this.endpoint = endpoint;
    this.topics = this.initTopics(topics);
  }

  initTopics(topics) {
    const result = [];
    Object.keys(topics).forEach(key => {
      if (topics[key].topicRead) {
        result.push(topics[key].topicRead);
      }
    });
    return result;
  }

  updateConfig(endpoint, topics, handler) {
    this.endpoint = endpoint;
    this.topics = this.initTopics(topics);

    if (this.consumer) {

      this.consumer.close(() => {
        this.consumer = this.createClient();
        this.startConsumer(handler);
      });
    }
  }

  createClient() {
    const listenTopics = [];
    this.topics.forEach(function(currTopic) { listenTopics.push({ topic: currTopic, partition: 0 }); });
    const options = {
      kafkaHost: this.endpoint,
      groupId: `${this.groupPrefix}`,
      sessionTimeout: SESSION_TIMEOUT,
      protocol: ['roundrobin'],
      autoCommit: true
    };
    // const consumer = new this.kafka.Consumer(new this.kafka.Client(this.endpoint), listenTopics, options);
    const consumer = new this.kafka.ConsumerGroup(options, this.topics);
    this.log.debug('Consumer created');
    return consumer;
  }

  startConsumer(handleData, retries = MAX_RETRIES) {

    try {
      this.log.debug('Starting consumer');
      this.consumer = this.createClient();
      this.log.info('Waiting for msgs');
      this.consumer.on('message', function (message) {
        const data = JSON.parse(message.value);
        handleData(message.key, message.topic, data);
      });

      const log = this.log;
      this.consumer.on('error', function (err) {
        log.error('consumer error: %s', err);
      });

    }
    catch (err) {
      this.log.error('Error on kafka consumer:' + err);
      if (retries > 0) {
        retries--;
        this.startConsumer(handleData, retries);
      }
    }
  }
}