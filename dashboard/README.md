# Shopzada Analytics Dashboard

Interactive web dashboard for visualizing Shopzada data warehouse analytics with 7 department-specific views.

## Quick Start

### Prerequisites
- [ ] Python 3.8+
- [ ] PostgreSQL database running (port 5433)
- [ ] Modern web browser

### Setup (First Time Only)

```bash
# 1. Navigate to dashboard folder
cd dashboard

# 2. Create virtual environment
python3 -m venv .venv

# 3. Activate virtual environment
source .venv/bin/activate  # macOS/Linux
# or
.venv\Scripts\activate     # Windows

# 4. Install dependencies
pip install -r requirements.txt
```

### Run Dashboard

```bash
# 1. Activate virtual environment (if not already active)
source .venv/bin/activate  # macOS/Linux

# 2. Start Flask API server
python3 api.py

# 3. Open index.html in your browser
# Option A: Double-click index.html
# Option B: Run local server (recommended)
python3 -m http.server 8000
# Then visit: http://localhost:8000
```

### Verify It's Working

```bash
# Check API health
curl http://localhost:5000/api/health
# Should return: {"status": "healthy", "database": "connected"}
```

## Dashboard Tabs

1. **📈 Overview** - Key business metrics and trends
2. **🏪 Business** - Product analytics
3. **📦 Operations** - Order fulfillment and delivery
4. **📢 Marketing** - Campaign performance
5. **👥 Customer Management** - Customer demographics
6. **🏢 Enterprise** - Merchant and staff performance
7. **📊 Table Counts** - Data warehouse table statistics

## Features

- **Date Range Filtering**: Filter all data by date range
- **Interactive Charts**: Line, bar, pie, and doughnut charts
- **Real-time Updates**: Click refresh to reload latest data
- **Responsive Design**: Works on desktop, tablet, and mobile
- **Data Quality Metrics**: View row counts and completeness percentages

---

## Troubleshooting

### API won't start
```bash
# Make sure virtual environment is activated
source .venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Database connection failed
```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Verify port 5433 is correct (not 5432)
# Default config uses port 5433
```

### Dashboard shows "Loading..." forever
1. Check Flask API is running at http://localhost:5000
2. Check browser console (F12) for errors
3. Verify database has data:
   ```sql
   SELECT COUNT(*) FROM fact_orders;
   ```

### CORS errors
- Use HTTP server instead of opening file directly:
  ```bash
  python3 -m http.server 8000
  ```

---

## Configuration

### Database Connection

Default settings (in `api.py`):
```python
DB_HOST=localhost
DB_PORT=5433
DB_NAME=kestra
DB_USER=kestra
DB_PASS=k3str4
```

To override, set environment variables:
```bash
export DB_HOST=your_host
export DB_PORT=your_port
export DB_NAME=your_database
export DB_USER=your_user
export DB_PASS=your_password
```

---

## API Endpoints

### Date Filtering
All endpoints support optional date parameters:
- `?start_date=2023-01-01&end_date=2023-12-31`

### Overview
- `GET /api/overview/metrics` - Total revenue, orders, customers, products
- `GET /api/overview/revenue-trend` - Revenue by month
- `GET /api/overview/top-products` - Top 10 products by revenue
- `GET /api/overview/customer-regions` - Customers by region

### Business
- `GET /api/business/metrics` - Product statistics
- `GET /api/business/categories` - Products by category
- `GET /api/business/price-distribution` - Price ranges
- `GET /api/business/top-products` - Top 20 products

### Operations
- `GET /api/operations/metrics` - Order and delivery metrics
- `GET /api/operations/monthly-orders` - Orders by month
- `GET /api/operations/delivery-performance` - Delivery stats
- `GET /api/operations/value-distribution` - Order values
- `GET /api/operations/top-merchants` - Top merchants

### Marketing
- `GET /api/marketing/metrics` - Campaign statistics
- `GET /api/marketing/campaign-performance` - Top campaigns
- `GET /api/marketing/discount-distribution` - Discount ranges
- `GET /api/marketing/trend` - Campaign usage over time

### Customer
- `GET /api/customer/metrics` - Customer statistics
- `GET /api/customer/countries` - Customers by country
- `GET /api/customer/age-distribution` - Age groups
- `GET /api/customer/gender-distribution` - Gender split
- `GET /api/customer/growth` - New customers over time

### Enterprise
- `GET /api/enterprise/metrics` - Merchant and staff stats
- `GET /api/enterprise/top-merchants` - Top merchants by revenue
- `GET /api/enterprise/merchant-countries` - Merchant locations
- `GET /api/enterprise/top-staff` - Top staff by orders
- `GET /api/enterprise/staff-levels` - Staff by job level

### Table Counts
- `GET /api/tablecount/all` - Row counts for all dim/fact tables

### System
- `GET /api/health` - Health check

---

## File Structure

```
dashboard/
├── index.html          # Dashboard UI
├── styles.css          # Styling
├── app.js             # Frontend logic & charts
├── api.py             # Flask API backend
├── requirements.txt   # Python dependencies
└── README.md          # This file
```

## Tech Stack

- **Frontend**: HTML5, CSS3, Vanilla JavaScript
- **Charts**: Chart.js 4.4.0
- **Backend**: Flask 3.0.0
- **Database**: PostgreSQL (via psycopg 3.2+)

---

## License

Part of the Shopzada Data Warehouse Final Project - 3CSE Group Sierra
