import logger from 'logger';

class Server {
  constructor() {

  }

  start() {
    logger.info('starting server...');
  }

  stop() {
    logger.info('server is going down...');
  }

  getClientNum() {
    return 5;
  }
}

const server = new Server();

export default server;