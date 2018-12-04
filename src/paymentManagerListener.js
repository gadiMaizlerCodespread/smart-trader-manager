module.exports = class PaymentManagerListener {
  constructor({ paymentManagerController, kafkaListener, logger, config  }) {
    this.logger = logger;
    this.config = config;
    this.paymentManagerController = paymentManagerController;
    this.kafkaListener = kafkaListener;
    this.controllerMsgMap = {};
    this.controllerMsgMap[this.config.externalRequests.topicRead] = paymentManagerController.onSendOrder.bind(paymentManagerController);
    this.controllerMsgMap[this.config.generateAddresses.topicRead] = paymentManagerController.onAdressesGenerated.bind(paymentManagerController);
    this.controllerMsgMap[this.config.depositsTracker.topicRead + '_' + this.config.depositsTracker.readKeys[0]] =
      paymentManagerController.onDeposit.bind(paymentManagerController);
    this.controllerMsgMap[this.config.depositsTracker.topicRead + '_' + this.config.depositsTracker.readKeys[1]] =
      paymentManagerController.onDepositsComplete.bind(paymentManagerController);
    this.controllerMsgMap[this.config.tradeManager.topicRead + '_' + this.config.tradeManager.readKeys[0]] =
      paymentManagerController.onTrade.bind(paymentManagerController);
    this.controllerMsgMap[this.config.tradeManager.topicRead + '_' + this.config.tradeManager.readKeys[1]] =
      paymentManagerController.onTradeComplete.bind(paymentManagerController);
    kafkaListener.startConsumer(this.messageRequest.bind(this));
  }
  messageRequest(key, topic, message) {
    this.logger.info('Key: <%s>, Topic: <%s>, Message: <%o>', key, topic, message);
    let map_key;
    if (key) {
      map_key = topic + '_' + key;
    }
    else {
      map_key = topic;
    }

    if (map_key in this.controllerMsgMap) {
      this.controllerMsgMap[map_key](message);
    }
    else {
      this.logger.warn('Unknown Kafka msg key: <%s>', map_key);
    }
  }
}