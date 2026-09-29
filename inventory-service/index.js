const amqp = require('amqplib');
const BROKER_URL = process.env.BROKER_URL || 'amqp://guest:guest@message-broker:5672';

async function start(retries = 30) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(BROKER_URL);
      const ch = await conn.createChannel();
      const exchange = 'order.exchange';
      const queue = 'inventory_service_queue';

      await ch.assertExchange(exchange, 'topic', { durable: true });
      await ch.assertQueue(queue, { durable: true });
      await ch.bindQueue(queue, exchange, 'order.placed');

      console.log('[Inventory] Listening on inventory_service_queue');

      ch.consume(queue, async (msg) => {
        if (!msg) return;
        const order = JSON.parse(msg.content.toString());
        console.log(`[Inventory] Reserved ${order.quantity} × ${order.product} → ${order.orderId}`);
        await new Promise(r => setTimeout(r, 300));
        ch.ack(msg);
      }, { noAck: false });

      return;
    } catch (e) {
      console.log(`[Inventory] Waiting RabbitMQ (${i + 1}/${retries})`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  throw new Error('RabbitMQ unavailable');
}

start().catch(e => {
  console.error(e);
  process.exit(1);
});