const express = require('express');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = 3000;
const BROKER_URL = process.env.BROKER_URL || 'amqp://guest:guest@message-broker:5672';

app.use(express.json());
let channel;

async function connectBroker(retries = 30) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(BROKER_URL);
      channel = await conn.createChannel();
      await channel.assertExchange('order.exchange', 'topic', { durable: true });
      console.log('[Order] Connected to RabbitMQ');
      return;
    } catch (e) {
      console.log(`[Order] Waiting RabbitMQ (${i + 1}/${retries})`);
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  throw new Error('RabbitMQ unavailable');
}

app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.post('/orders', async (req, res) => {
  try {
    const { product, quantity, customer } = req.body;
    if (!product || !quantity || !customer) {
      return res.status(400).json({ message: 'Missing fields' });
    }
    if (!channel) return res.status(503).json({ message: 'Broker not ready' });

    const order = {
      orderId: uuidv4(),
      product,
      quantity,
      customer,
      status: 'placed',
      createdAt: new Date().toISOString()
    };

    channel.publish(
      'order.exchange',
      'order.placed',
      Buffer.from(JSON.stringify(order)),
      { persistent: true }
    );

    console.log('[Order] Published order.placed →', order.orderId);
    res.status(201).json(order);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});

connectBroker().then(() => {
  app.listen(PORT, () => console.log(`[Order] Running on :${PORT}`));
}).catch(e => {
  console.error(e);
  process.exit(1);
});