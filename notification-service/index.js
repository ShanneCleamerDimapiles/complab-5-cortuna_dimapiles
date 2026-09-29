const amqp = require('amqplib');
const BROKER_URL = process.env.BROKER_URL || 'amqp://guest:guest@message-broker:5672';

async function start(retries = 30) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(BROKER_URL);
      const ch = await conn.createChannel();
      const exchange = 'order.exchange';

      // order.placed queue
      const orderQ = 'notification_service_queue';
      await ch.assertExchange(exchange, 'topic', { durable: true });
      await ch.assertQueue(orderQ, { durable: true });
      await ch.bindQueue(orderQ, exchange, 'order.placed');

      // payment.success queue
      const payQ = 'notification_payment_queue';
      await ch.assertQueue(payQ, { durable: true });
      await ch.bindQueue(payQ, exchange, 'payment.success');

      console.log('[Notification] Listening:');
      console.log('  • notification_service_queue  (order.placed)');
      console.log('  • notification_payment_queue  (payment.success)');

      ch.consume(orderQ, (msg) => {
        if (!msg) return;
        const o = JSON.parse(msg.content.toString());
        console.log(`[Notification] 📧 Order confirmation → ${o.orderId} (${o.customer})`);
        ch.ack(msg);
      }, { noAck: false });

      ch.consume(payQ, (msg) => {
        if (!msg) return;
        const p = JSON.parse(msg.content.toString());
        console.log(`[Notification] 💳 Payment success → ${p.orderId} (₱${p.amount})`);
        ch.ack(msg);
      }, { noAck: false });

      return;
    } catch (e) {
      console.log(`[Notification] Waiting RabbitMQ (${i + 1}/${retries})`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  throw new Error('RabbitMQ unavailable');
}

start().catch(e => {
  console.error(e);
  process.exit(1);
});