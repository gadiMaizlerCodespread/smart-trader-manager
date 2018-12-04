import kafka from 'kafka-node';
import ConfigManager from 'node-config-module';
import logger from 'logger';
import KafkaListener from './services/kafka-listener';
import KafkaProducer from './services/kafka-producer';
import PaymentManagerListener from './paymentManagerListener';
import PaymentManagerController from './paymentManagerController';

process.title = ['Smart Trader Manager'];
logger.info('Starting Smart Trader Manager');
let topics = {};
let endpoint = 'localhost:9092';
ConfigManager.init({}, './config/manager.json', () => {
  const currentConfig = ConfigManager.getConfig();
  topics = currentConfig.kafka.topics;
});

const groupPrefix = 'smartTradeManager';
const kafkaProducer = new KafkaProducer({ groupPrefix, endpoint, topics, kafka, logger });
kafkaProducer.startProducer();
const kafkaListener = new KafkaListener({ groupPrefix, endpoint, topics, kafka, logger });
const paymentManagerController = new PaymentManagerController({ logger, config: topics, kafka, producer: kafkaProducer });
const paymentManagerListener = new PaymentManagerListener({ paymentManagerController, kafkaListener, logger, config: topics  });
bootstrapHealth({ kafkaListener });

const terminateAll = () => {
  kafkaListener.close();
};

process.on('SIGTERM', () => {
  logger.info('SIGTERM signal received.');
  // close all resources here
  terminateAll();
});

function bootstrapHealth(kafkaListener) {
  // kafkaListener.testConnection()
}