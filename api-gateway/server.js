const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
const PORT = 8080;
const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || 'http://order-service:3000';
const CORS_ORIGIN = process.env.CORS_ORIGIN || 'http://localhost:3000';

app.use(cors({ origin: CORS_ORIGIN }));
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'api-gateway' });
});

app.post('/orders', async (req, res) => {
  const { product, quantity, customer } = req.body;

  if (!product || !quantity || !customer) {
    return res.status(400).json({ message: 'product, quantity and customer are required' });
  }

  try {
    const response = await fetch(`${ORDER_SERVICE_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ product, quantity, customer })
    });
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (err) {
    console.error('[Gateway]', err.message);
    res.status(502).json({ message: 'Order service unavailable' });
  }
});

app.listen(PORT, () => console.log(`API Gateway running on :${PORT}`));