module.exports = class PaymentManagerController {
  constructor({  logger, config, producer  }) {
    this.logger = logger;
    this.config = config;
    this.orders = {};
    this.producer = producer;
  }
  onSendOrder(message) { 
    this.logger.info('Sent Order: <%o>', message);
    if ('tradeOrderId' in message) {
      message.status = 'Created';
      this.orders[message.tradeOrderId] = message;
      this.orders[message.tradeOrderId].orderCreatedTime = message.timestamp;
      const depositMessage = {
        tradeOrderId: message.tradeOrderId,
        account: message.account,
        action: message.action,
        assetPair: message.assetPair,
        size: message.size
      };
      this.producer.sendMessage(this.config.generateAddresses.topicWrite, null, depositMessage);
      this.producer.sendMessage(this.config.dbListener.topicWrite, this.config.dbListener.writeKeys[0], this.orders[message.tradeOrderId]);
    }
    this.logger.debug('onSendOrder All Orders: <%o>', this.orders);
  }

  onAdressesGenerated(message)  {
    this.logger.info('Adresses Generated for Order: <%o>', message);
    if ('tradeOrderId' in message && message.tradeOrderId in this.orders) {
      this.orders[message.tradeOrderId].status = 'Waiting for deposits';
      this.orders[message.tradeOrderId].addressesGeneratedTime = message.timestamp;
      const addressesMessage = {
        tradeOrderId: message.tradeOrderId,
        actionType: this.orders[message.tradeOrderId].actionType,
        assetPair: this.orders[message.tradeOrderId].assetPair,
        size: this.orders[message.tradeOrderId].size,
        depositPlan: []
      };
      const responseDepositPlan = [];
      message.depositPlan.array.forEach(element => {
        addressesMessage.depositPlan.push({ depositAddress: element.depositAddress, size: element.size, exchange: element.exchange });
        responseDepositPlan.depositPlan.push({ depositAddress: element.depositAddress, size: element.size });
      });
      this.orders[message.tradeOrderId].depositPlan = addressesMessage.depositPlan;
      this.logger.debug('Sending deposit addresses: <%o>', addressesMessage);
      this.producer.sendMessage(this.config.depositsTracker.topicWrite, null, addressesMessage);
      addressesMessage.depositPlan = responseDepositPlan;
      this.producer.sendMessage(this.config.externalRequests.topicWrite, null, addressesMessage);
      this.producer.sendMessage(this.config.dbListener.topicWrite, this.config.dbListener.writeKeys[0], this.orders[message.tradeOrderId]);
    }
    this.logger.debug('onAdressesGenerated All Orders: <%o>', this.orders);
  }

  onDeposit(message) {
    this.logger.info('Deposit received for Order: <%o>', message);
    if ('tradeOrderId' in message && message.tradeOrderId in this.orders) {
      if (this.orders[message.tradeOrderId].status == 'Waiting for deposits') {
        this.orders[message.tradeOrderId].status = 'Getting deposits';
        this.orders[message.tradeOrderId].firstDepositTime = message.timestamp;
        const tradeOrder = {
          tradeOrderId: message.tradeOrderId,
          exchanges: [],
          durationMinutes: this.orders[message.tradeOrderId].durationMinutes,
          price: this.orders[message.tradeOrderId].price,
          size: this.orders[message.tradeOrderId].size,
          actionType: this.orders[message.tradeOrderId].actionType,
          account: this.orders[message.tradeOrderId].account,
        };
        this.orders[message.tradeOrderId].depositPlan.array.forEach(element => {
          tradeOrder.exchanges.push(element.exchange);
          element.depositSize = 0;
        });
        this.logger.debug('All Orders: <%o>', this.orders);
        this.producer.sendMessage(this.config.tradeManager.topicWrite, this.config.tradeManager.writeKeys[0], tradeOrder);
        this.producer.sendMessage(this.config.dbListener.topicWrite, this.config.dbListener.writeKeys[0], this.orders[message.tradeOrderId]);
      }
      this.orders[message.tradeOrderId].depositPlan[message.exchange] += message.size;
      this.orders[message.tradeOrderId].lastDepositTime = message.timestamp;
      const deposit = {
        tradeOrderId: message.tradeOrderId,
        account: this.orders[message.tradeOrderId].account,
        exchange: message.exchange,
        size: message.size
      };
      this.producer.sendMessage(this.config.tradeManager.topicWrite, this.config.tradeManager.writeKeys[1], deposit);
      this.producer.sendMessage(this.config.dbListener.topicWrite, this.config.dbListener.writeKeys[0], this.orders[message.tradeOrderId]);
    }
    this.logger.debug('onDeposit All Orders: <%o>', this.orders);
  }
  onDepositsComplete(message) {
    this.logger.info('Deposits complete for Order: <%o>', message);
    if ('tradeOrderId' in message && message.tradeOrderId in this.orders) {
      this.orders[message.tradeOrderId].status == 'Deposits Complete';
      this.orders[message.tradeOrderId].depositCompleteTime = message.timestamp;
      this.producer.sendMessage(this.config.dbListener.topicWrite, this.config.dbListener.writeKeys[0], this.orders[message.tradeOrderId]);
    }
    this.logger.debug('onDepositsComplete All Orders: <%o>', this.orders);
  }

  onTrade(message) {
    this.logger.info('On trade for Order: <%o>', message);
    if ('tradeOrderId' in message && message.tradeOrderId in this.orders) {
      const tradeOrder = this.orders[message.tradeOrderId];
      if (!tradeOrder.firstTradeTime) {
        this.orders[message.tradeOrderId].firstTradeTime = message.timestamp;
      }
      tradeOrder.lastTradeTime = message.timestamp;
      this.producer.sendMessage(this.config.dbListener.topicWrite, this.config.dbListener.writeKeys[0], this.orders[message.tradeOrderId]);
    }
    this.logger.debug('onTrade All Orders: <%o>', this.orders);
  }

  onTradeComplete(message) {
    this.logger.info('Trade complete for Order: <%o>', message);
    if ('tradeOrderId' in message && message.tradeOrderId in this.orders) {
      this.orders[message.tradeOrderId].status == 'Trade Complete';
      this.orders[message.tradeOrderId].tradeCompleteTime = message.timestamp;
      this.producer.sendMessage(this.config.dbListener.topicWrite, this.config.dbListener.writeKeys[0], this.orders[message.tradeOrderId]);
    }
    this.logger.debug('onTradeComplete All Orders: <%o>', this.orders);
  }
};