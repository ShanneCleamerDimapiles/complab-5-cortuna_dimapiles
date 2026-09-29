const amqp = require('amqplib');
const BROKER_URL = process.env.BROKER_URL || 'amqp://guest:guest@message-broker:5672';

async function start(retries = 30) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(BROKER_URL);
      const ch = await conn.createChannel();
      const exchange = 'order.exchange';
      const queue = 'payment_service_queue';

      await ch.assertExchange(exchange, 'topic', { durable: true });
      await ch.assertQueue(queue, { durable: true });
      await ch.bindQueue(queue, exchange, 'order.placed');

      console.log('[Payment] Listening on payment_service_queue');

      ch.consume(queue, async (msg) => {
        if (!msg) return;
        const order = JSON.parse(msg.content.toString());
        console.log('[Payment] Received →', order.orderId);

        await new Promise(r => setTimeout(r, 400));

        const payment = {
          orderId: order.orderId,
          product: order.product,
          quantity: order.quantity,
          customer: order.customer,
          amount: order.quantity * 100,
          status: 'success',
          paidAt: new Date().toISOString()
        };

        ch.publish(
          exchange,
          'payment.success',
          Buffer.from(JSON.stringify(payment)),
          { persistent: true }
        );

        console.log('[Payment] Published payment.success →', order.orderId);
        ch.ack(msg);
      }, { noAck: false });

      return;
    } catch (e) {
      console.log(`[Payment] Waiting RabbitMQ (${i + 1}/${retries})`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  throw new Error('RabbitMQ unavailable');
}

start().catch(e => {
  console.error(e);
  process.exit(1);
});