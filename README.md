# 📊 Pyramid Leaderboards Backend

Welcome to the **Pyramid Leaderboards Backend**! This Node.js application serves as the backend for the [Pyramid Performance Tracker](https://github.com/Pyramid-IT-Learn/pyramid-leaderboards), providing essential APIs for managing and retrieving leaderboard data.

## 📦 Getting Started

### Prerequisites

- Node.js (version 14 or above)
- MongoDB URI (for connecting to your MongoDB instance)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Pyramid-IT-Learn/pyramid-leaderboards-backend.git
   cd pyramid-leaderboards-backend
   ```

2. Install the dependencies:
   ```bash
   npm install
   ```

3. Set up your environment variables:
   Create a `.env` file in the root directory with the following content:
   ```
   MONGODB_URI=your_mongodb_uri
   PORT=5000
   ```

4. Start the server:
   ```bash
   npm start
   ```

The server will run on `http://localhost:5000`.

## 📚 API Endpoints

Here are the available API endpoints you can use:

- **GET /databases**: List all databases
- **GET /databases/:db/collections**: List all collections in a specified database
- **GET /databases/:db/collections/:collection/data**: Get all data from a specified collection
- **GET /databases/:db/collections/:collection/batch-update-time**: Get the last update time of documents in a specified collection
- **GET /endpoints**: List all available endpoints
- **GET /**: Root route that returns a simple greeting

## 🤝 Contributing

Contributions are welcome! If you have suggestions for improvements or new features, feel free to open an issue or submit a pull request.

## 💬 Support

If you encounter any issues or have questions, please feel free to reach out via GitHub issues.
