# Tombola Ticket Management System

A Streamlit-based web application for managing tombola (lottery) ticket sales and orders. The system can import data from Jimdo Excel exports, manage ticket assignments, and export data for printing.

## Features

- **Excel Import**: Upload and parse Jimdo Excel export files
- **Ticket Management**: Automatically assign ticket IDs and track orders
- **Data Export**: Generate Excel files with one row per ticket
- **Database Storage**: MySQL-based data persistence
- **Web Interface**: Clean Streamlit-based UI

## Database Migration

This project has been migrated from SQLite to PostgreSQL to support cloud deployment. The migration includes:

- Updated database client with PostgreSQL connector (psycopg2)
- Environment variable configuration for database connections
- Support for both individual parameters and connection URIs
- Automatic table creation and schema management

## Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL database (local or hosted)
- Gmail API credentials (for email functionality)
- Required Python packages (see `pyproject.toml`)

### Local Development

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd tombola
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Set up environment variables**:
   ```bash
   cp env.example .env
   # Edit .env with your database credentials
   ```

4. **Run the application**:
   ```bash
   streamlit run app.py
   ```

### Environment Variables

Create a `.env` file with your database configuration:

```bash
# Database Configuration
DB_HOST=your-postgresql-host.com
DB_PORT=5432
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=tombola

# Alternative: Connection URI
# DATABASE_URL=postgresql://user:password@host:port/database
```

## Deployment

This application is designed to be deployed on Streamlit Cloud. See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

## Project Structure

```
tombola/
├── app.py              # Main Streamlit application
├── sql_client.py       # PostgreSQL database client
├── parse_jimdo.py      # Excel parsing logic
├── gmail_client.py     # Gmail integration (if needed)
├── pyproject.toml      # Project dependencies
├── requirements.txt    # Streamlit Cloud requirements
├── .streamlit/         # Streamlit configuration
├── env.example         # Environment variables template
└── DEPLOYMENT.md       # Deployment guide
```

## Database Schema

The application automatically creates a `tickets` table with the following structure:

```sql
CREATE TABLE tickets (
    id INT,
    date VARCHAR(255),
    firm VARCHAR(255) NULL,
    name VARCHAR(255),
    email VARCHAR(255),
    num_tickets INT,
    achat VARCHAR(255) NULL,
    UNIQUE KEY unique_name_date (name, date)
);
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

[Add your license here]
