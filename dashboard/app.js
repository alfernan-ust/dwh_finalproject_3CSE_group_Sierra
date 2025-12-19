const API_BASE_URL = 'http://localhost:5000/api';

const charts = {};

let dateFilter = {
    startDate: null,
    endDate: null
};

function switchTab(tabName) {
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });

    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });

    document.getElementById(`${tabName}-tab`).classList.add('active');

    event.target.classList.add('active');

    loadTabData(tabName);
}

function loadTabData(tabName) {
    switch (tabName) {
        case 'overview':
            loadOverviewData();
            break;
        case 'business':
            loadBusinessData();
            break;
        case 'operations':
            loadOperationsData();
            break;
        case 'marketing':
            loadMarketingData();
            break;
        case 'customer':
            loadCustomerData();
            break;
        case 'enterprise':
            loadEnterpriseData();
            break;
        case 'tablecount':
            loadTableCountData();
            break;
    }
}

async function fetchData(endpoint) {
    try {
        let url = `${API_BASE_URL}/${endpoint}`;

        if (dateFilter.startDate && dateFilter.endDate) {
            const separator = endpoint.includes('?') ? '&' : '?';
            url += `${separator}start_date=${dateFilter.startDate}&end_date=${dateFilter.endDate}`;
        }

        const response = await fetch(url);
        if (!response.ok) throw new Error('Network response was not ok');
        return await response.json();
    } catch (error) {
        console.error('Error fetching data:', error);
        return null;
    }
}

function applyDateFilter() {
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;

    if (!startDate || !endDate) {
        alert('Please select both start and end dates');
        return;
    }

    if (startDate > endDate) {
        alert('Start date must be before end date');
        return;
    }

    dateFilter.startDate = startDate;
    dateFilter.endDate = endDate;

    clearAllCharts();

    const activeTab = document.querySelector('.tab-btn.active');
    const tabName = activeTab.textContent.toLowerCase().split(' ')[1] || 'overview';
    loadTabData(tabName);

    updateLastUpdated();
}

function resetDateFilter() {
    dateFilter.startDate = null;
    dateFilter.endDate = null;

    document.getElementById('start-date').value = '2023-01-01';
    document.getElementById('end-date').value = '2023-12-31';

    clearAllCharts();

    const activeTab = document.querySelector('.tab-btn.active');
    const tabName = activeTab.textContent.toLowerCase().split(' ')[1] || 'overview';
    loadTabData(tabName);

    updateLastUpdated();
}

function clearAllCharts() {
    Object.keys(charts).forEach(key => {
        if (charts[key]) {
            charts[key].destroy();
            delete charts[key];
        }
    });
}

function formatCurrency(value) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(value);
}

function formatNumber(value) {
    return new Intl.NumberFormat('en-US').format(value);
}

async function loadOverviewData() {
    try {
        const [metrics, revenueTrend, topProducts, customerRegions] = await Promise.all([
            fetchData('overview/metrics'),
            fetchData('overview/revenue-trend'),
            fetchData('overview/top-products'),
            fetchData('overview/customer-regions')
        ]);

        if (metrics) {
            document.getElementById('total-revenue').textContent = formatCurrency(metrics.total_revenue);
            document.getElementById('total-orders').textContent = formatNumber(metrics.total_orders);
            document.getElementById('active-customers').textContent = formatNumber(metrics.active_customers);
            document.getElementById('products-sold').textContent = formatNumber(metrics.products_sold);
        }

        if (revenueTrend && !charts.revenueTrend) {
            const ctx = document.getElementById('revenue-trend-chart').getContext('2d');
            charts.revenueTrend = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: revenueTrend.labels,
                    datasets: [{
                        label: 'Revenue',
                        data: revenueTrend.values,
                        borderColor: '#2563eb',
                        backgroundColor: 'rgba(37, 99, 235, 0.1)',
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: {
                            display: false
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                callback: function(value) {
                                    return '$' + value.toLocaleString();
                                }
                            }
                        }
                    }
                }
            });
        }

        if (!charts.ordersStatus) {
            const ctx = document.getElementById('orders-status-chart').getContext('2d');
            charts.ordersStatus = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['Completed', 'Processing', 'Delayed'],
                    datasets: [{
                        data: [450000, 35000, 15000],
                        backgroundColor: ['#10b981', '#3b82f6', '#f59e0b']
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (topProducts && !charts.topProducts) {
            const ctx = document.getElementById('top-products-chart').getContext('2d');
            charts.topProducts = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: topProducts.labels,
                    datasets: [{
                        label: 'Revenue',
                        data: topProducts.values,
                        backgroundColor: '#7c3aed'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    indexAxis: 'y',
                    plugins: {
                        legend: {
                            display: false
                        }
                    }
                }
            });
        }

        if (customerRegions && !charts.customerRegion) {
            const ctx = document.getElementById('customer-region-chart').getContext('2d');
            charts.customerRegion = new Chart(ctx, {
                type: 'pie',
                data: {
                    labels: customerRegions.labels,
                    datasets: [{
                        data: customerRegions.values,
                        backgroundColor: [
                            '#2563eb',
                            '#7c3aed',
                            '#10b981',
                            '#f59e0b',
                            '#ef4444'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }
    } catch (error) {
        console.error('Error loading overview data:', error);
    }
}

async function loadBusinessData() {
    try {
        const [metrics, categories, priceDistribution, topProducts] = await Promise.all([
            fetchData('business/metrics'),
            fetchData('business/categories'),
            fetchData('business/price-distribution'),
            fetchData('business/top-products')
        ]);

        if (metrics) {
            document.getElementById('business-total-products').textContent = formatNumber(metrics.total_products);
            document.getElementById('business-avg-price').textContent = formatCurrency(metrics.avg_price);
            document.getElementById('business-best-product').textContent = metrics.best_product || 'N/A';
            document.getElementById('business-incomplete').textContent = metrics.incomplete || '0';
        }

        if (categories && !charts.businessCategories) {
            const ctx = document.getElementById('business-category-chart').getContext('2d');
            charts.businessCategories = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: categories.labels,
                    datasets: [{
                        data: categories.values,
                        backgroundColor: [
                            '#2563eb',
                            '#7c3aed',
                            '#10b981',
                            '#f59e0b',
                            '#ef4444',
                            '#06b6d4'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (priceDistribution && !charts.businessPriceDist) {
            const ctx = document.getElementById('business-price-dist-chart').getContext('2d');
            charts.businessPriceDist = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: priceDistribution.labels,
                    datasets: [{
                        label: 'Products',
                        data: priceDistribution.values,
                        backgroundColor: '#10b981'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (topProducts && !charts.businessTopProducts) {
            const ctx = document.getElementById('business-top-products-chart').getContext('2d');
            charts.businessTopProducts = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: topProducts.labels,
                    datasets: [{
                        label: 'Revenue',
                        data: topProducts.values,
                        backgroundColor: '#2563eb'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    indexAxis: 'y'
                }
            });
        }
    } catch (error) {
        console.error('Error loading business data:', error);
    }
}

async function loadOperationsData() {
    try {
        const [metrics, monthlyOrders, delivery, valueDist, topMerchants] = await Promise.all([
            fetchData('operations/metrics'),
            fetchData('operations/monthly-orders'),
            fetchData('operations/delivery-performance'),
            fetchData('operations/value-distribution'),
            fetchData('operations/top-merchants')
        ]);

        if (metrics) {
            document.getElementById('ops-total-orders').textContent = formatNumber(metrics.total_orders);
            document.getElementById('ops-avg-delivery').textContent = `${metrics.avg_delivery_days} days`;
            document.getElementById('ops-delayed-orders').textContent = formatNumber(metrics.delayed_orders);
            document.getElementById('ops-avg-value').textContent = formatCurrency(metrics.avg_order_value);
        }

        if (monthlyOrders && !charts.opsMonthlyOrders) {
            const ctx = document.getElementById('ops-monthly-orders-chart').getContext('2d');
            charts.opsMonthlyOrders = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: monthlyOrders.labels,
                    datasets: [{
                        label: 'Orders',
                        data: monthlyOrders.values,
                        borderColor: '#2563eb',
                        backgroundColor: 'rgba(37, 99, 235, 0.1)',
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (delivery && !charts.opsDelivery) {
            const ctx = document.getElementById('ops-delivery-chart').getContext('2d');
            charts.opsDelivery = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: delivery.labels,
                    datasets: [{
                        label: 'Orders',
                        data: delivery.values,
                        backgroundColor: ['#10b981', '#3b82f6', '#f59e0b']
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (valueDist && !charts.opsValueDist) {
            const ctx = document.getElementById('ops-value-dist-chart').getContext('2d');
            charts.opsValueDist = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: valueDist.labels,
                    datasets: [{
                        label: 'Orders',
                        data: valueDist.values,
                        backgroundColor: '#7c3aed'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (topMerchants && !charts.opsTopMerchants) {
            const ctx = document.getElementById('ops-top-merchants-chart').getContext('2d');
            charts.opsTopMerchants = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: topMerchants.labels,
                    datasets: [{
                        label: 'Orders',
                        data: topMerchants.values,
                        backgroundColor: '#10b981'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    indexAxis: 'y'
                }
            });
        }
    } catch (error) {
        console.error('Error loading operations data:', error);
    }
}

async function loadMarketingData() {
    try {
        const [metrics, performance, discountDist, trend] = await Promise.all([
            fetchData('marketing/metrics'),
            fetchData('marketing/campaign-performance'),
            fetchData('marketing/discount-distribution'),
            fetchData('marketing/trend')
        ]);

        if (metrics) {
            document.getElementById('marketing-active-campaigns').textContent = metrics.active_campaigns || '0';
            document.getElementById('marketing-utilization').textContent = `${metrics.utilization}%`;
            document.getElementById('marketing-discounts').textContent = formatCurrency(metrics.total_discounts);
            document.getElementById('marketing-avg-discount').textContent = `${metrics.avg_discount}%`;
        }

        if (performance && !charts.marketingPerformance) {
            const ctx = document.getElementById('marketing-performance-chart').getContext('2d');
            charts.marketingPerformance = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: performance.labels,
                    datasets: [{
                        label: 'Usage Count',
                        data: performance.values,
                        backgroundColor: '#7c3aed'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    indexAxis: 'y'
                }
            });
        }

        if (discountDist && !charts.marketingDiscountDist) {
            const ctx = document.getElementById('marketing-discount-dist-chart').getContext('2d');
            charts.marketingDiscountDist = new Chart(ctx, {
                type: 'pie',
                data: {
                    labels: discountDist.labels,
                    datasets: [{
                        data: discountDist.values,
                        backgroundColor: [
                            '#2563eb',
                            '#7c3aed',
                            '#10b981',
                            '#f59e0b',
                            '#ef4444'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (trend && !charts.marketingTrend) {
            const ctx = document.getElementById('marketing-trend-chart').getContext('2d');
            charts.marketingTrend = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: trend.labels,
                    datasets: [{
                        label: 'Campaign Usage',
                        data: trend.values,
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }
    } catch (error) {
        console.error('Error loading marketing data:', error);
    }
}

async function loadCustomerData() {
    try {
        const [metrics, countries, ageDist, genderDist, growth] = await Promise.all([
            fetchData('customer/metrics'),
            fetchData('customer/countries'),
            fetchData('customer/age-distribution'),
            fetchData('customer/gender-distribution'),
            fetchData('customer/growth')
        ]);

        if (metrics) {
            document.getElementById('customer-total').textContent = formatNumber(metrics.total_customers);
            document.getElementById('customer-top-region').textContent = metrics.top_region || 'N/A';
            document.getElementById('customer-avg-age').textContent = metrics.avg_age ? `${metrics.avg_age} years` : 'N/A';
            document.getElementById('customer-gender-split').textContent = metrics.gender_split || 'N/A';
        }

        if (countries && !charts.customerCountry) {
            const ctx = document.getElementById('customer-country-chart').getContext('2d');
            charts.customerCountry = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: countries.labels,
                    datasets: [{
                        label: 'Customers',
                        data: countries.values,
                        backgroundColor: '#2563eb'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    indexAxis: 'y'
                }
            });
        }

        if (ageDist && !charts.customerAge) {
            const ctx = document.getElementById('customer-age-chart').getContext('2d');
            charts.customerAge = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: ageDist.labels,
                    datasets: [{
                        label: 'Customers',
                        data: ageDist.values,
                        backgroundColor: '#10b981'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (genderDist && !charts.customerGender) {
            const ctx = document.getElementById('customer-gender-chart').getContext('2d');
            charts.customerGender = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: genderDist.labels,
                    datasets: [{
                        data: genderDist.values,
                        backgroundColor: ['#3b82f6', '#ec4899', '#8b5cf6']
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (growth && !charts.customerGrowth) {
            const ctx = document.getElementById('customer-growth-chart').getContext('2d');
            charts.customerGrowth = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: growth.labels,
                    datasets: [{
                        label: 'New Customers',
                        data: growth.values,
                        borderColor: '#7c3aed',
                        backgroundColor: 'rgba(124, 58, 237, 0.1)',
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }
    } catch (error) {
        console.error('Error loading customer data:', error);
    }
}

async function loadEnterpriseData() {
    try {
        const [metrics, topMerchants, merchantCountries, topStaff, staffLevels] = await Promise.all([
            fetchData('enterprise/metrics'),
            fetchData('enterprise/top-merchants'),
            fetchData('enterprise/merchant-countries'),
            fetchData('enterprise/top-staff'),
            fetchData('enterprise/staff-levels')
        ]);

        if (metrics) {
            document.getElementById('enterprise-merchants').textContent = formatNumber(metrics.total_merchants);
            document.getElementById('enterprise-staff').textContent = formatNumber(metrics.total_staff);
            document.getElementById('enterprise-top-merchant').textContent = metrics.top_merchant || 'N/A';
            document.getElementById('enterprise-top-staff').textContent = metrics.top_staff || 'N/A';
        }

        if (topMerchants && !charts.enterpriseMerchants) {
            const ctx = document.getElementById('enterprise-merchants-chart').getContext('2d');
            charts.enterpriseMerchants = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: topMerchants.labels,
                    datasets: [{
                        label: 'Revenue',
                        data: topMerchants.values,
                        backgroundColor: '#2563eb'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    indexAxis: 'y'
                }
            });
        }

        if (merchantCountries && !charts.enterpriseMerchantCountry) {
            const ctx = document.getElementById('enterprise-merchant-country-chart').getContext('2d');
            charts.enterpriseMerchantCountry = new Chart(ctx, {
                type: 'pie',
                data: {
                    labels: merchantCountries.labels,
                    datasets: [{
                        data: merchantCountries.values,
                        backgroundColor: [
                            '#2563eb',
                            '#7c3aed',
                            '#10b981',
                            '#f59e0b',
                            '#ef4444'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }

        if (topStaff && !charts.enterpriseStaff) {
            const ctx = document.getElementById('enterprise-staff-chart').getContext('2d');
            charts.enterpriseStaff = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: topStaff.labels,
                    datasets: [{
                        label: 'Orders Handled',
                        data: topStaff.values,
                        backgroundColor: '#10b981'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    indexAxis: 'y'
                }
            });
        }

        if (staffLevels && !charts.enterpriseStaffLevel) {
            const ctx = document.getElementById('enterprise-staff-level-chart').getContext('2d');
            charts.enterpriseStaffLevel = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: staffLevels.labels,
                    datasets: [{
                        data: staffLevels.values,
                        backgroundColor: [
                            '#2563eb',
                            '#7c3aed',
                            '#10b981',
                            '#f59e0b'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true
                }
            });
        }
    } catch (error) {
        console.error('Error loading enterprise data:', error);
    }
}

async function loadTableCountData() {
    try {
        const data = await fetchData('tablecount/all');

        if (!data) return;

        if (data.summary) {
            document.getElementById('total-dim-tables').textContent = data.summary.total_dim_tables;
            document.getElementById('total-fact-tables').textContent = data.summary.total_fact_tables;
            document.getElementById('total-dim-rows').textContent = data.summary.total_dim_rows.toLocaleString();
            document.getElementById('total-fact-rows').textContent = data.summary.total_fact_rows.toLocaleString();
        }

        if (data.dimension_tables) {
            const dimTableBody = document.querySelector('#dim-tables-table tbody');
            dimTableBody.innerHTML = data.dimension_tables.map(table => `
                <tr>
                    <td><strong>${table.table_name}</strong></td>
                    <td>${table.total_rows.toLocaleString()}</td>
                    <td>${table.incomplete_rows.toLocaleString()}</td>
                    <td>${table.complete_percentage}%</td>
                </tr>
            `).join('');
        }

        if (data.fact_tables) {
            const factTableBody = document.querySelector('#fact-tables-table tbody');
            factTableBody.innerHTML = data.fact_tables.map(table => `
                <tr>
                    <td><strong>${table.table_name}</strong></td>
                    <td>${table.total_rows.toLocaleString()}</td>
                    <td>${table.incomplete_rows.toLocaleString()}</td>
                    <td>${table.complete_percentage}%</td>
                </tr>
            `).join('');
        }

        if (!charts.allTables) {
            const allTables = [...data.dimension_tables, ...data.fact_tables];
            const ctx = document.getElementById('all-tables-chart').getContext('2d');
            charts.allTables = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: allTables.map(t => t.table_name),
                    datasets: [{
                        label: 'Total Rows',
                        data: allTables.map(t => t.total_rows),
                        backgroundColor: allTables.map(t =>
                            t.table_name.startsWith('dim_') ? '#2563eb' : '#10b981'
                        )
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }
    } catch (error) {
        console.error('Error loading table count data:', error);
    }
}

function refreshData() {
    const activeTab = document.querySelector('.tab-btn.active').textContent.toLowerCase().split(' ')[1] || 'overview';
    loadTabData(activeTab);
    updateLastUpdated();
}

function updateLastUpdated() {
    const now = new Date();
    document.getElementById('last-updated').textContent = `Last Updated: ${now.toLocaleString()}`;
}

document.addEventListener('DOMContentLoaded', function() {
    updateLastUpdated();
    loadOverviewData();
});
