"""
Flask API for Shopzada Analytics Dashboard
Provides data endpoints for the dashboard
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg
from psycopg.rows import dict_row
import os

app = Flask(__name__)
CORS(app) 


DB_CONNSTRING = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
    host=os.getenv('DB_HOST', 'localhost'),
    port=os.getenv('DB_PORT', '5433'),  
    dbname=os.getenv('DB_NAME', 'kestra'),
    user=os.getenv('DB_USER', 'kestra'),
    password=os.getenv('DB_PASS', 'k3str4')
)

def execute_query(query, params=None):
    """Execute a query and return results"""
    try:
        with psycopg.connect(DB_CONNSTRING, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                return results
    except Exception as e:
        print(f"Database error: {e}")
        return []


@app.route('/api/overview/metrics')
def overview_metrics():
    """Get overview metrics"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                ROUND(SUM(f.net_total)::NUMERIC, 2) as total_revenue,
                COUNT(*) as total_orders,
                COUNT(DISTINCT f.customer_key) as active_customers,
                (SELECT COUNT(DISTINCT li.product_key)
                 FROM fact_line_items li
                 JOIN fact_orders fo ON li.order_key = fo.order_key
                 JOIN dim_date d2 ON fo.transaction_date_key = d2.date_key
                 WHERE d2.date_actual BETWEEN %(start_date)s AND %(end_date)s) as products_sold
            FROM fact_orders f
            JOIN dim_date d ON f.transaction_date_key = d.date_key
            WHERE f.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
        """
        result = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                ROUND(SUM(net_total)::NUMERIC, 2) as total_revenue,
                COUNT(*) as total_orders,
                COUNT(DISTINCT customer_key) as active_customers,
                (SELECT COUNT(DISTINCT product_key) FROM fact_line_items) as products_sold
            FROM fact_orders
            WHERE is_incomplete = FALSE
        """
        result = execute_query(query)

    if result:
        return jsonify(result[0])
    return jsonify({})

@app.route('/api/overview/revenue-trend')
def overview_revenue_trend():
    """Get revenue trend over last 12 months of available data"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                TO_CHAR(d.date_actual, 'Mon YYYY') as month,
                d.year_actual,
                d.month_actual,
                ROUND(SUM(f.net_total)::NUMERIC, 2) as revenue
            FROM fact_orders f
            JOIN dim_date d ON f.transaction_date_key = d.date_key
            WHERE f.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY month, d.year_actual, d.month_actual
            ORDER BY d.year_actual, d.month_actual
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            WITH ranked_months AS (
                SELECT
                    TO_CHAR(d.date_actual, 'Mon YYYY') as month,
                    d.year_actual,
                    d.month_actual,
                    ROUND(SUM(f.net_total)::NUMERIC, 2) as revenue
                FROM fact_orders f
                JOIN dim_date d ON f.transaction_date_key = d.date_key
                WHERE f.is_incomplete = FALSE
                GROUP BY month, d.year_actual, d.month_actual
                ORDER BY d.year_actual DESC, d.month_actual DESC
                LIMIT 12
            )
            SELECT month, revenue
            FROM ranked_months
            ORDER BY year_actual, month_actual
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['month'] for r in results],
        'values': [float(r['revenue'] or 0) for r in results]
    })

@app.route('/api/overview/top-products')
def overview_top_products():
    """Get top 10 products by revenue"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                p.product_name,
                ROUND(SUM(li.net_total)::NUMERIC, 2) as revenue
            FROM fact_line_items li
            JOIN dim_product p ON li.product_key = p.product_key
            JOIN fact_orders o ON li.order_key = o.order_key
            JOIN dim_date d ON o.transaction_date_key = d.date_key
            WHERE li.is_incomplete = FALSE
              AND p.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 10
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                p.product_name,
                ROUND(SUM(li.net_total)::NUMERIC, 2) as revenue
            FROM fact_line_items li
            JOIN dim_product p ON li.product_key = p.product_key
            WHERE li.is_incomplete = FALSE
              AND p.is_incomplete = FALSE
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 10
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['product_name'] for r in results],
        'values': [float(r['revenue'] or 0) for r in results]
    })

@app.route('/api/overview/customer-regions')
def overview_customer_regions():
    """Get customer distribution by region"""
    query = """
        SELECT
            country,
            COUNT(*) as customer_count
        FROM dim_customer
        WHERE is_incomplete = FALSE
          AND country IS NOT NULL
        GROUP BY country
        ORDER BY customer_count DESC
        LIMIT 5
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['country'] for r in results],
        'values': [r['customer_count'] for r in results]
    })


@app.route('/api/business/metrics')
def business_metrics():
    """Get business metrics"""
    query = """
        SELECT
            COUNT(*) as total_products,
            ROUND(AVG(price)::NUMERIC, 2) as avg_price,
            (SELECT product_name FROM dim_product p
             JOIN fact_line_items li ON p.product_key = li.product_key
             WHERE p.is_incomplete = FALSE
             GROUP BY p.product_name
             ORDER BY SUM(li.net_total) DESC
             LIMIT 1) as best_product,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete
        FROM dim_product
    """
    result = execute_query(query)
    if result:
        return jsonify(result[0])
    return jsonify({})

@app.route('/api/business/categories')
def business_categories():
    """Get product distribution by category"""
    query = """
        SELECT
            product_type,
            COUNT(*) as count
        FROM dim_product
        WHERE is_incomplete = FALSE
          AND product_type IS NOT NULL
        GROUP BY product_type
        ORDER BY count DESC
        LIMIT 10
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['product_type'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/business/price-distribution')
def business_price_distribution():
    """Get price distribution"""
    query = """
        SELECT
            CASE
                WHEN price < 10 THEN '$0-$10'
                WHEN price < 20 THEN '$10-$20'
                WHEN price < 30 THEN '$20-$30'
                WHEN price < 40 THEN '$30-$40'
                ELSE '$40+'
            END as price_range,
            COUNT(*) as count
        FROM dim_product
        WHERE is_incomplete = FALSE
          AND price IS NOT NULL
        GROUP BY price_range
        ORDER BY MIN(price)
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['price_range'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/business/top-products')
def business_top_products():
    """Get top 20 products by revenue"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                p.product_name,
                ROUND(SUM(li.net_total)::NUMERIC, 2) as revenue
            FROM fact_line_items li
            JOIN dim_product p ON li.product_key = p.product_key
            JOIN fact_orders o ON li.order_key = o.order_key
            JOIN dim_date d ON o.transaction_date_key = d.date_key
            WHERE li.is_incomplete = FALSE
              AND p.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 20
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                p.product_name,
                ROUND(SUM(li.net_total)::NUMERIC, 2) as revenue
            FROM fact_line_items li
            JOIN dim_product p ON li.product_key = p.product_key
            WHERE li.is_incomplete = FALSE
              AND p.is_incomplete = FALSE
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 20
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['product_name'] for r in results],
        'values': [float(r['revenue'] or 0) for r in results]
    })


@app.route('/api/operations/metrics')
def operations_metrics():
    """Get operations metrics"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                COUNT(*) as total_orders,
                ROUND(AVG(f.delay_in_days)::NUMERIC, 1) as avg_delivery_days,
                SUM(CASE WHEN f.delay_in_days > 0 THEN 1 ELSE 0 END) as delayed_orders,
                ROUND(AVG(f.net_total)::NUMERIC, 2) as avg_order_value
            FROM fact_orders f
            JOIN dim_date d ON f.transaction_date_key = d.date_key
            WHERE f.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
        """
        result = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                COUNT(*) as total_orders,
                ROUND(AVG(delay_in_days)::NUMERIC, 1) as avg_delivery_days,
                SUM(CASE WHEN delay_in_days > 0 THEN 1 ELSE 0 END) as delayed_orders,
                ROUND(AVG(net_total)::NUMERIC, 2) as avg_order_value
            FROM fact_orders
            WHERE is_incomplete = FALSE
        """
        result = execute_query(query)

    if result:
        return jsonify(result[0])
    return jsonify({})

@app.route('/api/operations/monthly-orders')
def operations_monthly_orders():
    """Get monthly orders trend for last 12 months of data"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                TO_CHAR(d.date_actual, 'Mon YYYY') as month,
                d.year_actual,
                d.month_actual,
                COUNT(*) as order_count
            FROM fact_orders f
            JOIN dim_date d ON f.transaction_date_key = d.date_key
            WHERE f.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY month, d.year_actual, d.month_actual
            ORDER BY d.year_actual, d.month_actual
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            WITH ranked_months AS (
                SELECT
                    TO_CHAR(d.date_actual, 'Mon YYYY') as month,
                    d.year_actual,
                    d.month_actual,
                    COUNT(*) as order_count
                FROM fact_orders f
                JOIN dim_date d ON f.transaction_date_key = d.date_key
                WHERE f.is_incomplete = FALSE
                GROUP BY month, d.year_actual, d.month_actual
                ORDER BY d.year_actual DESC, d.month_actual DESC
                LIMIT 12
            )
            SELECT month, order_count
            FROM ranked_months
            ORDER BY year_actual, month_actual
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['month'] for r in results],
        'values': [r['order_count'] for r in results]
    })

@app.route('/api/operations/delivery-performance')
def operations_delivery_performance():
    """Get delivery performance"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                CASE
                    WHEN f.delay_in_days = 0 THEN 'On Time'
                    WHEN f.delay_in_days BETWEEN 1 AND 3 THEN 'Slightly Delayed'
                    ELSE 'Significantly Delayed'
                END as status,
                COUNT(*) as count
            FROM fact_orders f
            JOIN dim_date d ON f.transaction_date_key = d.date_key
            WHERE f.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY status
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                CASE
                    WHEN delay_in_days = 0 THEN 'On Time'
                    WHEN delay_in_days BETWEEN 1 AND 3 THEN 'Slightly Delayed'
                    ELSE 'Significantly Delayed'
                END as status,
                COUNT(*) as count
            FROM fact_orders
            WHERE is_incomplete = FALSE
            GROUP BY status
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['status'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/operations/value-distribution')
def operations_value_distribution():
    """Get order value distribution"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                CASE
                    WHEN f.net_total < 50 THEN '$0-$50'
                    WHEN f.net_total < 100 THEN '$50-$100'
                    WHEN f.net_total < 200 THEN '$100-$200'
                    WHEN f.net_total < 500 THEN '$200-$500'
                    ELSE '$500+'
                END as value_range,
                COUNT(*) as count
            FROM fact_orders f
            JOIN dim_date d ON f.transaction_date_key = d.date_key
            WHERE f.is_incomplete = FALSE
              AND f.net_total IS NOT NULL
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY value_range
            ORDER BY MIN(f.net_total)
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                CASE
                    WHEN net_total < 50 THEN '$0-$50'
                    WHEN net_total < 100 THEN '$50-$100'
                    WHEN net_total < 200 THEN '$100-$200'
                    WHEN net_total < 500 THEN '$200-$500'
                    ELSE '$500+'
                END as value_range,
                COUNT(*) as count
            FROM fact_orders
            WHERE is_incomplete = FALSE
              AND net_total IS NOT NULL
            GROUP BY value_range
            ORDER BY MIN(net_total)
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['value_range'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/operations/top-merchants')
def operations_top_merchants():
    """Get top merchants by order count"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                m.name as merchant_name,
                COUNT(*) as order_count
            FROM fact_orders f
            JOIN dim_merchant m ON f.merchant_key = m.merchant_key
            JOIN dim_date d ON f.transaction_date_key = d.date_key
            WHERE f.is_incomplete = FALSE
              AND m.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY m.name
            ORDER BY order_count DESC
            LIMIT 10
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                m.name as merchant_name,
                COUNT(*) as order_count
            FROM fact_orders f
            JOIN dim_merchant m ON f.merchant_key = m.merchant_key
            WHERE f.is_incomplete = FALSE
              AND m.is_incomplete = FALSE
            GROUP BY m.name
            ORDER BY order_count DESC
            LIMIT 10
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['merchant_name'] for r in results],
        'values': [r['order_count'] for r in results]
    })


@app.route('/api/marketing/metrics')
def marketing_metrics():
    """Get marketing metrics"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                (SELECT COUNT(*) FROM dim_campaign WHERE is_incomplete = FALSE) as active_campaigns,
                ROUND(100.0 * COUNT(CASE WHEN ct.availed THEN 1 END) / NULLIF(COUNT(*), 0), 1) as utilization,
                ROUND(SUM(o.discount_total)::NUMERIC, 2) as total_discounts,
                ROUND(AVG(c.discount)::NUMERIC, 1) as avg_discount
            FROM fact_campaign_transactions ct
            LEFT JOIN fact_orders o ON ct.order_key = o.order_key
            LEFT JOIN dim_campaign c ON ct.campaign_key = c.campaign_key
            LEFT JOIN dim_date d ON o.transaction_date_key = d.date_key
            WHERE ct.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
        """
        result = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                (SELECT COUNT(*) FROM dim_campaign WHERE is_incomplete = FALSE) as active_campaigns,
                ROUND(100.0 * COUNT(CASE WHEN availed THEN 1 END) / NULLIF(COUNT(*), 0), 1) as utilization,
                ROUND(SUM(o.discount_total)::NUMERIC, 2) as total_discounts,
                ROUND(AVG(c.discount)::NUMERIC, 1) as avg_discount
            FROM fact_campaign_transactions ct
            LEFT JOIN fact_orders o ON ct.order_key = o.order_key
            LEFT JOIN dim_campaign c ON ct.campaign_key = c.campaign_key
            WHERE ct.is_incomplete = FALSE
        """
        result = execute_query(query)

    if result:
        return jsonify(result[0])
    return jsonify({})
@app.route('/api/marketing/campaign-performance')
def marketing_campaign_performance():
    """Get campaign performance"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                c.campaign_name,
                COUNT(*) as usage_count
            FROM fact_campaign_transactions ct
            JOIN dim_campaign c ON ct.campaign_key = c.campaign_key
            JOIN fact_orders o ON ct.order_key = o.order_key
            JOIN dim_date d ON o.transaction_date_key = d.date_key
            WHERE ct.availed = TRUE
              AND ct.is_incomplete = FALSE
              AND c.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY c.campaign_name
            ORDER BY usage_count DESC
            LIMIT 10
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                c.campaign_name,
                COUNT(*) as usage_count
            FROM fact_campaign_transactions ct
            JOIN dim_campaign c ON ct.campaign_key = c.campaign_key
            WHERE ct.availed = TRUE
              AND ct.is_incomplete = FALSE
              AND c.is_incomplete = FALSE
            GROUP BY c.campaign_name
            ORDER BY usage_count DESC
            LIMIT 10
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['campaign_name'] for r in results],
        'values': [r['usage_count'] for r in results]
    })

@app.route('/api/marketing/discount-distribution')
def marketing_discount_distribution():
    """Get discount distribution"""
    query = """
        SELECT
            CASE
                WHEN discount < 5 THEN '0-5%'
                WHEN discount < 10 THEN '5-10%'
                WHEN discount < 20 THEN '10-20%'
                WHEN discount < 30 THEN '20-30%'
                ELSE '30%+'
            END as discount_range,
            COUNT(*) as count
        FROM dim_campaign
        WHERE is_incomplete = FALSE
          AND discount IS NOT NULL
        GROUP BY discount_range
        ORDER BY MIN(discount)
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['discount_range'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/marketing/trend')
def marketing_trend():
    """Get campaign usage trend for last 12 months of data"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                TO_CHAR(d.date_actual, 'Mon YYYY') as month,
                d.year_actual,
                d.month_actual,
                COUNT(*) as usage_count
            FROM fact_campaign_transactions ct
            JOIN fact_orders o ON ct.order_key = o.order_key
            JOIN dim_date d ON o.transaction_date_key = d.date_key
            WHERE ct.availed = TRUE
              AND ct.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY month, d.year_actual, d.month_actual
            ORDER BY d.year_actual, d.month_actual
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            WITH ranked_months AS (
                SELECT
                    TO_CHAR(d.date_actual, 'Mon YYYY') as month,
                    d.year_actual,
                    d.month_actual,
                    COUNT(*) as usage_count
                FROM fact_campaign_transactions ct
                JOIN fact_orders o ON ct.order_key = o.order_key
                JOIN dim_date d ON o.transaction_date_key = d.date_key
                WHERE ct.availed = TRUE
                  AND ct.is_incomplete = FALSE
                GROUP BY month, d.year_actual, d.month_actual
                ORDER BY d.year_actual DESC, d.month_actual DESC
                LIMIT 12
            )
            SELECT month, usage_count
            FROM ranked_months
            ORDER BY year_actual, month_actual
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['month'] for r in results],
        'values': [r['usage_count'] for r in results]
    })


@app.route('/api/customer/metrics')
def customer_metrics():
    """Get customer metrics"""
    query = """
        SELECT
            COUNT(*) as total_customers,
            (SELECT country FROM dim_customer
             WHERE is_incomplete = FALSE
               AND country IS NOT NULL
             GROUP BY country
             ORDER BY COUNT(*) DESC
             LIMIT 1) as top_region,
            ROUND(AVG(age)::NUMERIC, 0) as avg_age,
            (SELECT gender FROM dim_customer
             WHERE is_incomplete = FALSE
               AND gender IS NOT NULL
             GROUP BY gender
             ORDER BY COUNT(*) DESC
             LIMIT 1) as gender_split
        FROM dim_customer
        WHERE is_incomplete = FALSE
    """
    result = execute_query(query)
    if result:
        return jsonify(result[0])
    return jsonify({})

@app.route('/api/customer/countries')
def customer_countries():
    """Get customer distribution by country"""
    query = """
        SELECT
            country,
            COUNT(*) as count
        FROM dim_customer
        WHERE is_incomplete = FALSE
          AND country IS NOT NULL
        GROUP BY country
        ORDER BY count DESC
        LIMIT 10
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['country'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/customer/age-distribution')
def customer_age_distribution():
    """Get age distribution"""
    query = """
        SELECT
            CASE
                WHEN age < 20 THEN '< 20'
                WHEN age < 30 THEN '20-29'
                WHEN age < 40 THEN '30-39'
                WHEN age < 50 THEN '40-49'
                WHEN age < 60 THEN '50-59'
                ELSE '60+'
            END as age_group,
            COUNT(*) as count
        FROM dim_customer
        WHERE is_incomplete = FALSE
          AND age IS NOT NULL
        GROUP BY age_group
        ORDER BY MIN(age)
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['age_group'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/customer/gender-distribution')
def customer_gender_distribution():
    """Get gender distribution"""
    query = """
        SELECT
            gender,
            COUNT(*) as count
        FROM dim_customer
        WHERE is_incomplete = FALSE
          AND gender IS NOT NULL
        GROUP BY gender
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['gender'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/customer/growth')
def customer_growth():
    """Get customer growth over time for last 12 months of data"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                TO_CHAR(creation_date, 'Mon YYYY') as month,
                EXTRACT(YEAR FROM creation_date)::INTEGER as year,
                EXTRACT(MONTH FROM creation_date)::INTEGER as month_num,
                COUNT(*) as new_customers
            FROM dim_customer
            WHERE is_incomplete = FALSE
              AND creation_date IS NOT NULL
              AND creation_date BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY month, year, month_num
            ORDER BY year, month_num
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            WITH ranked_months AS (
                SELECT
                    TO_CHAR(creation_date, 'Mon YYYY') as month,
                    EXTRACT(YEAR FROM creation_date)::INTEGER as year,
                    EXTRACT(MONTH FROM creation_date)::INTEGER as month_num,
                    COUNT(*) as new_customers
                FROM dim_customer
                WHERE is_incomplete = FALSE
                  AND creation_date IS NOT NULL
                GROUP BY month, year, month_num
                ORDER BY year DESC, month_num DESC
                LIMIT 12
            )
            SELECT month, new_customers
            FROM ranked_months
            ORDER BY year, month_num
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['month'] for r in results],
        'values': [r['new_customers'] for r in results]
    })


@app.route('/api/enterprise/metrics')
def enterprise_metrics():
    """Get enterprise metrics"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                (SELECT COUNT(*) FROM dim_merchant WHERE is_incomplete = FALSE) as total_merchants,
                (SELECT COUNT(*) FROM dim_staff WHERE is_incomplete = FALSE) as total_staff,
                (SELECT m.name FROM dim_merchant m
                 JOIN fact_orders o ON m.merchant_key = o.merchant_key
                 JOIN dim_date d ON o.transaction_date_key = d.date_key
                 WHERE m.is_incomplete = FALSE
                   AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
                 GROUP BY m.name
                 ORDER BY SUM(o.net_total) DESC
                 LIMIT 1) as top_merchant,
                (SELECT s.name FROM dim_staff s
                 JOIN fact_orders o ON s.staff_key = o.staff_key
                 JOIN dim_date d ON o.transaction_date_key = d.date_key
                 WHERE s.is_incomplete = FALSE
                   AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
                 GROUP BY s.name
                 ORDER BY COUNT(*) DESC
                 LIMIT 1) as top_staff
        """
        result = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                (SELECT COUNT(*) FROM dim_merchant WHERE is_incomplete = FALSE) as total_merchants,
                (SELECT COUNT(*) FROM dim_staff WHERE is_incomplete = FALSE) as total_staff,
                (SELECT m.name FROM dim_merchant m
                 JOIN fact_orders o ON m.merchant_key = o.merchant_key
                 WHERE m.is_incomplete = FALSE
                 GROUP BY m.name
                 ORDER BY SUM(o.net_total) DESC
                 LIMIT 1) as top_merchant,
                (SELECT s.name FROM dim_staff s
                 JOIN fact_orders o ON s.staff_key = o.staff_key
                 WHERE s.is_incomplete = FALSE
                 GROUP BY s.name
                 ORDER BY COUNT(*) DESC
                 LIMIT 1) as top_staff
        """
        result = execute_query(query)

    if result:
        return jsonify(result[0])
    return jsonify({})

@app.route('/api/enterprise/top-merchants')
def enterprise_top_merchants():
    """Get top merchants by revenue"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                m.name as merchant_name,
                ROUND(SUM(o.net_total)::NUMERIC, 2) as revenue
            FROM fact_orders o
            JOIN dim_merchant m ON o.merchant_key = m.merchant_key
            JOIN dim_date d ON o.transaction_date_key = d.date_key
            WHERE o.is_incomplete = FALSE
              AND m.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY m.name
            ORDER BY revenue DESC
            LIMIT 10
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                m.name as merchant_name,
                ROUND(SUM(o.net_total)::NUMERIC, 2) as revenue
            FROM fact_orders o
            JOIN dim_merchant m ON o.merchant_key = m.merchant_key
            WHERE o.is_incomplete = FALSE
              AND m.is_incomplete = FALSE
            GROUP BY m.name
            ORDER BY revenue DESC
            LIMIT 10
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['merchant_name'] for r in results],
        'values': [float(r['revenue'] or 0) for r in results]
    })

@app.route('/api/enterprise/merchant-countries')
def enterprise_merchant_countries():
    """Get merchant distribution by country"""
    query = """
        SELECT
            country,
            COUNT(*) as count
        FROM dim_merchant
        WHERE is_incomplete = FALSE
          AND country IS NOT NULL
        GROUP BY country
        ORDER BY count DESC
        LIMIT 5
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['country'] for r in results],
        'values': [r['count'] for r in results]
    })

@app.route('/api/enterprise/top-staff')
def enterprise_top_staff():
    """Get top staff by orders handled"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if start_date and end_date:
        query = """
            SELECT
                s.name as staff_name,
                COUNT(*) as orders_handled
            FROM fact_orders o
            JOIN dim_staff s ON o.staff_key = s.staff_key
            JOIN dim_date d ON o.transaction_date_key = d.date_key
            WHERE o.is_incomplete = FALSE
              AND s.is_incomplete = FALSE
              AND d.date_actual BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY s.name
            ORDER BY orders_handled DESC
            LIMIT 10
        """
        results = execute_query(query, {'start_date': start_date, 'end_date': end_date})
    else:
        query = """
            SELECT
                s.name as staff_name,
                COUNT(*) as orders_handled
            FROM fact_orders o
            JOIN dim_staff s ON o.staff_key = s.staff_key
            WHERE o.is_incomplete = FALSE
              AND s.is_incomplete = FALSE
            GROUP BY s.name
            ORDER BY orders_handled DESC
            LIMIT 10
        """
        results = execute_query(query)

    return jsonify({
        'labels': [r['staff_name'] for r in results],
        'values': [r['orders_handled'] for r in results]
    })

@app.route('/api/enterprise/staff-levels')
def enterprise_staff_levels():
    """Get staff distribution by job level"""
    query = """
        SELECT
            job_level,
            COUNT(*) as count
        FROM dim_staff
        WHERE is_incomplete = FALSE
          AND job_level IS NOT NULL
        GROUP BY job_level
        ORDER BY count DESC
    """
    results = execute_query(query)
    return jsonify({
        'labels': [r['job_level'] for r in results],
        'values': [r['count'] for r in results]
    })


@app.route('/api/tablecount/all')
def tablecount_all():
    """Get row counts for all dimension and fact tables"""

    dim_tables_query = """
        SELECT
            'dim_campaign' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM dim_campaign
        UNION ALL
        SELECT
            'dim_customer' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM dim_customer
        UNION ALL
        SELECT
            'dim_date' as table_name,
            COUNT(*) as total_rows,
            0 as incomplete_rows
        FROM dim_date
        UNION ALL
        SELECT
            'dim_merchant' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM dim_merchant
        UNION ALL
        SELECT
            'dim_product' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM dim_product
        UNION ALL
        SELECT
            'dim_staff' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM dim_staff
        ORDER BY table_name
    """

    fact_tables_query = """
        SELECT
            'fact_orders' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM fact_orders
        UNION ALL
        SELECT
            'fact_line_items' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM fact_line_items
        UNION ALL
        SELECT
            'fact_campaign_transactions' as table_name,
            COUNT(*) as total_rows,
            SUM(CASE WHEN is_incomplete THEN 1 ELSE 0 END) as incomplete_rows
        FROM fact_campaign_transactions
        ORDER BY table_name
    """

    dim_results = execute_query(dim_tables_query)
    fact_results = execute_query(fact_tables_query)

    total_dim_tables = len(dim_results)
    total_fact_tables = len(fact_results)
    total_dim_rows = sum(r['total_rows'] for r in dim_results)
    total_fact_rows = sum(r['total_rows'] for r in fact_results)

    return jsonify({
        'dimension_tables': [
            {
                'table_name': r['table_name'],
                'total_rows': r['total_rows'],
                'incomplete_rows': r['incomplete_rows'],
                'complete_percentage': round((r['total_rows'] - r['incomplete_rows']) / r['total_rows'] * 100, 2) if r['total_rows'] > 0 else 0
            }
            for r in dim_results
        ],
        'fact_tables': [
            {
                'table_name': r['table_name'],
                'total_rows': r['total_rows'],
                'incomplete_rows': r['incomplete_rows'],
                'complete_percentage': round((r['total_rows'] - r['incomplete_rows']) / r['total_rows'] * 100, 2) if r['total_rows'] > 0 else 0
            }
            for r in fact_results
        ],
        'summary': {
            'total_dim_tables': total_dim_tables,
            'total_fact_tables': total_fact_tables,
            'total_dim_rows': total_dim_rows,
            'total_fact_rows': total_fact_rows
        }
    })

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        with psycopg.connect(DB_CONNSTRING) as conn:
            return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

if __name__ == '__main__':
    print("Starting Shopzada Analytics API...")
    print(f"Database: {os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'kestra')}")
    app.run(host='0.0.0.0', port=5000, debug=True)
