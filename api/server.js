const express = require('express');
const mongodb = require('mongodb');
const cors = require('cors');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 5000;

app.use(cors());

app.use(express.json());

const client = new mongodb.MongoClient(process.env.MONGODB_URI, { useNewUrlParser: true, useUnifiedTopology: true });

let isConnected = false;

const connectClient = async () => {
  if (!isConnected) {
    await client.connect();
    isConnected = true;
  }
};

// Endpoint to fetch all databases
app.get('/databases', async (req, res) => {
  try {
    console.log('-------------------');
    console.log('GET /databases');
    console.log('-------------------');

    await connectClient();
    const adminDb = client.db().admin();
    const { databases } = await adminDb.listDatabases();

    const filteredDatabases = databases
      .filter(db => !['admin', 'config', 'local'].includes(db.name))
      .map(db => db.name);

    res.json(filteredDatabases);
    console.log('Sent databases:', filteredDatabases);
  } catch (error) {
    console.error('Error fetching databases:', error);
    res.status(500).send('Error fetching databases');
  }
});

// Endpoint to fetch all collections for a specific database
app.get('/databases/:db/collections', async (req, res) => {
  try {
    const dbName = req.params.db;
    console.log('-------------------');
    console.log(`GET /databases/${dbName}/collections`);
    console.log('-------------------');

    await connectClient();
    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();
    const collectionNames = collections.map(col => col.name);

    res.json(collectionNames);
    console.log('Sent collections:', collectionNames);
  } catch (error) {
    console.error(`Error fetching collections for database ${req.params.db}:`, error);
    res.status(500).send('Error fetching collections');
  }
});

// Endpoint to fetch data from a specific collection in a specific database
app.get('/databases/:db/collections/:collection/data', async (req, res) => {
  try {
    const dbName = req.params.db;
    const collectionName = req.params.collection;
    console.log('-------------------');
    console.log(`GET /databases/${dbName}/collections/${collectionName}/data`);
    console.log('-------------------');

    await connectClient();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const data = await collection.find({}).sort({ Percentile: -1 }).toArray();

    res.json(data);
    console.log(`Sent ${data.length} documents from ${collectionName} in database ${dbName}`);
  } catch (error) {
    console.error(`Error fetching data from ${collectionName} in database ${dbName}:`, error);
    res.status(500).send('Error fetching data');
  }
});

// Endpoint to fetch the last update time of documents in a specific collection in a specific database
app.get('/databases/:db/collections/:collection/batch-update-time', async (req, res) => {
  try {
    const dbName = req.params.db;
    const collectionName = req.params.collection;
    console.log('-------------------');
    console.log(`GET /databases/${dbName}/collections/${collectionName}/batch-update-time`);
    console.log('-------------------');

    await connectClient();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const latestDocument = await collection.find({}).sort({ _id: -1 }).limit(1).toArray();

    if (latestDocument.length === 0) {
      return res.status(404).send('No documents found in this collection');
    }

    const latestObjectId = latestDocument[0]._id;
    const timestamp = latestObjectId.getTimestamp();
    
    res.json({ lastUpdateTime: timestamp });
    console.log(`Sent last update time for ${collectionName} in database ${dbName}: ${timestamp}`);
  } catch (error) {
    console.error(`Error fetching last update time from ${collectionName} in database ${dbName}:`, error);
    res.status(500).send('Error fetching last update time');
  }
});

// Endpoint to fetch the last update time of documents in a specific collection in a specific database
app.get('/databases/:db/collections/:collection/batch-update-time-oplog', async (req, res) => {
  try {
    const dbName = req.params.db;
    const collectionName = req.params.collection;
    console.log('-------------------');
    console.log(`GET /databases/${dbName}/collections/${collectionName}/batch-update-time-oplog`);
    console.log('-------------------');

    await connectClient();
    const db = client.db(dbName);
    
    // Access the oplog
    const oplog = client.db('local').collection('oplog.rs');
    
    // Find the last update for the specified collection
    const latestUpdate = await oplog.find({
      ns: `${dbName}.${collectionName}`,
      op: { $in: ['i', 'u'] } // 'i' for insert, 'u' for update
    }).sort({ ts: -1 }).limit(1).toArray();

    if (latestUpdate.length === 0) {
      return res.status(404).send('No updates found in this collection');
    }

    const timestamp = latestUpdate[0].ts.getTimestamp();
    
    res.json({ lastUpdateTime: timestamp });
    console.log(`Sent last update time for ${collectionName} in database ${dbName}: ${timestamp}`);
  } catch (error) {
    console.error(`Error fetching last update time from ${collectionName} in database ${dbName}:`, error);
    res.status(500).send('Error fetching last update time');
  }
});

// Root route
app.get('/', (req, res) => {
  res.send('Hello World!');
});

// New endpoint to list all available endpoints
app.get('/endpoints', (req, res) => {
  const endpoints = `
    Available endpoints:
    - GET /databases: List all databases
    - GET /databases/:db/collections: List all collections in a database
    - GET /databases/:db/collections/:collection/data: Get all data from a collection in a database
    - GET /databases/:db/collections/:collection/batch-update-time: Get the last update time of documents in a collection
    - GET /: Root route
  `;

  res.send(endpoints);
  console.log('Sent endpoints list');
});

// Start server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

module.exports = app; // Export the app
