#!/usr/bin/env python3
"""Generate realistic sample datasets for data pipeline testing."""

import json
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()
random.seed(42)
np.random.seed(42)


def generate_ecommerce_data():
    """Generate e-commerce sample data (customers, orders, products)."""
    
    # Generate customers
    customers = []
    for i in range(1000):
        customer = {
            'customer_id': f'C{i+1:06d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'country': fake.country(),
            'zip_code': fake.zipcode(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
            'acquisition_channel': random.choice(['Organic', 'Paid Search', 'Social Media', 'Email', 'Referral'])
        }
        customers.append(customer)
    
    # Generate products
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty', 'Toys']
    products = []
    for i in range(500):
        category = random.choice(categories)
        product = {
            'product_id': f'P{i+1:06d}',
            'product_name': fake.catch_phrase(),
            'category': category,
            'brand': fake.company(),
            'price': round(random.uniform(10, 1000), 2),
            'cost': round(random.uniform(5, 500), 2),
            'weight_kg': round(random.uniform(0.1, 10), 2),
            'dimensions': f"{random.randint(10,50)}x{random.randint(10,50)}x{random.randint(5,20)}",
            'color': fake.color_name(),
            'material': random.choice(['Plastic', 'Metal', 'Wood', 'Cotton', 'Polyester', 'Leather']),
            'launch_date': fake.date_between(start_date='-3y', end_date='-1m'),
            'is_active': random.choice([True, True, True, False])  # 75% active
        }
        products.append(product)
    
    # Generate orders
    orders = []
    order_items = []
    order_id = 1
    
    for _ in range(5000):
        customer = random.choice(customers)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        
        order = {
            'order_id': f'O{order_id:08d}',
            'customer_id': customer['customer_id'],
            'order_date': order_date,
            'status': random.choices(
                ['completed', 'shipped', 'processing', 'cancelled', 'returned'],
                weights=[70, 15, 10, 3, 2]
            )[0],
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery']),
            'shipping_address': fake.address().replace('\n', ', '),
            'shipping_cost': round(random.uniform(0, 50), 2),
            'tax_amount': 0,
            'total_amount': 0,
            'discount_code': fake.bothify(text='SAVE##') if random.random() < 0.3 else None,
            'discount_amount': 0
        }
        
        # Generate order items
        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 20, 8, 2])[0]
        selected_products = random.sample(products, num_items)
        
        total_amount = 0
        for product in selected_products:
            quantity = random.randint(1, 3)
            unit_price = product['price']
            
            order_item = {
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': quantity * unit_price
            }
            order_items.append(order_item)
            total_amount += order_item['total_price']
        
        # Calculate tax and discount
        if order['discount_code']:
            order['discount_amount'] = round(total_amount * random.uniform(0.05, 0.2), 2)
            total_amount -= order['discount_amount']
        
        order['tax_amount'] = round(total_amount * 0.08, 2)
        order['total_amount'] = round(total_amount + order['tax_amount'] + order['shipping_cost'], 2)
        
        orders.append(order)
        order_id += 1
    
    return customers, products, orders, order_items


def generate_finance_data():
    """Generate financial sample data (transactions, accounts, loans)."""
    
    # Generate accounts
    accounts = []
    for i in range(1000):
        account = {
            'account_id': f'A{i+1:08d}',
            'customer_id': f'C{random.randint(1, 500):06d}',
            'account_type': random.choice(['Checking', 'Savings', 'Credit', 'Investment']),
            'account_status': random.choices(['Active', 'Inactive', 'Frozen'], weights=[85, 10, 5])[0],
            'open_date': fake.date_between(start_date='-5y', end_date='-1m'),
            'balance': round(random.uniform(-1000, 50000), 2),
            'credit_limit': round(random.uniform(1000, 25000), 2) if random.random() < 0.3 else None,
            'interest_rate': round(random.uniform(0.01, 0.25), 4),
            'branch_id': f'B{random.randint(1, 50):03d}',
            'currency': random.choices(['USD', 'EUR', 'GBP'], weights=[70, 20, 10])[0]
        }
        accounts.append(account)
    
    # Generate transactions
    transactions = []
    transaction_id = 1
    
    for _ in range(20000):
        account = random.choice(accounts)
        transaction_date = fake.date_time_between(start_date='-1y', end_date='now')
        
        transaction_types = ['Deposit', 'Withdrawal', 'Transfer', 'Payment', 'Fee', 'Interest']
        transaction_type = random.choice(transaction_types)
        
        if transaction_type in ['Deposit', 'Interest']:
            amount = round(random.uniform(10, 5000), 2)
        elif transaction_type in ['Withdrawal', 'Payment', 'Fee']:
            amount = -round(random.uniform(5, 2000), 2)
        else:  # Transfer
            amount = round(random.uniform(50, 10000), 2) * random.choice([1, -1])
        
        transaction = {
            'transaction_id': f'T{transaction_id:10d}',
            'account_id': account['account_id'],
            'transaction_date': transaction_date,
            'transaction_type': transaction_type,
            'amount': amount,
            'balance_after': account['balance'],  # Simplified
            'description': fake.sentence(nb_words=6),
            'merchant_name': fake.company() if transaction_type == 'Payment' else None,
            'merchant_category': random.choice([
                'Grocery', 'Gas', 'Restaurant', 'Retail', 'Healthcare', 'Utilities'
            ]) if transaction_type == 'Payment' else None,
            'location': fake.city() if random.random() < 0.7 else None,
            'reference_number': fake.bothify(text='REF######'),
            'is_fraudulent': random.random() < 0.01  # 1% fraud rate
        }
        
        transactions.append(transaction)
        transaction_id += 1
    
    # Generate loans
    loans = []
    for i in range(300):
        loan = {
            'loan_id': f'L{i+1:08d}',
            'customer_id': f'C{random.randint(1, 500):06d}',
            'loan_type': random.choice(['Personal', 'Mortgage', 'Auto', 'Business', 'Student']),
            'loan_amount': round(random.uniform(5000, 500000), 2),
            'interest_rate': round(random.uniform(0.03, 0.15), 4),
            'term_months': random.choice([12, 24, 36, 60, 120, 240, 360]),
            'monthly_payment': 0,  # Calculate based on amount and rate
            'origination_date': fake.date_between(start_date='-3y', end_date='now'),
            'maturity_date': None,  # Calculate based on term
            'status': random.choices(['Current', 'Late', 'Default', 'Paid Off'], weights=[70, 15, 5, 10])[0],
            'collateral_type': random.choice(['Real Estate', 'Vehicle', 'None']) if random.random() < 0.6 else None,
            'credit_score': random.randint(300, 850),
            'debt_to_income_ratio': round(random.uniform(0.1, 0.6), 3)
        }
        
        # Calculate monthly payment (simplified)
        if loan['interest_rate'] > 0:
            monthly_rate = loan['interest_rate'] / 12
            loan['monthly_payment'] = round(
                loan['loan_amount'] * monthly_rate * (1 + monthly_rate) ** loan['term_months'] /
                ((1 + monthly_rate) ** loan['term_months'] - 1), 2
            )
        
        # Calculate maturity date
        loan['maturity_date'] = loan['origination_date'] + timedelta(days=loan['term_months'] * 30)
        
        loans.append(loan)
    
    return accounts, transactions, loans


def generate_iot_data():
    """Generate IoT sensor sample data."""
    
    # Generate devices
    devices = []
    device_types = ['Temperature', 'Humidity', 'Pressure', 'Motion', 'Light', 'Air Quality', 'Energy']
    
    for i in range(100):
        device = {
            'device_id': f'IOT{i+1:05d}',
            'device_name': f"{random.choice(device_types)} Sensor {i+1}",
            'device_type': random.choice(device_types),
            'location': fake.address().replace('\n', ', '),
            'building': f'Building {random.choice("ABCDEF")}',
            'floor': random.randint(1, 10),
            'room': f'Room {random.randint(100, 999)}',
            'manufacturer': random.choice(['SensorTech', 'IoTCorp', 'SmartDevices', 'TechSensors']),
            'model': fake.bothify(text='Model ###-??'),
            'firmware_version': f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}",
            'install_date': fake.date_between(start_date='-2y', end_date='-1m'),
            'last_maintenance': fake.date_between(start_date='-6m', end_date='today'),
            'is_active': random.choices([True, False], weights=[90, 10])[0],
            'battery_level': random.randint(10, 100) if random.random() < 0.7 else None
        }
        devices.append(device)
    
    # Generate sensor readings
    readings = []
    reading_id = 1
    
    # Generate readings for the last 30 days
    start_date = datetime.now() - timedelta(days=30)
    
    for device in devices:
        if not device['is_active']:
            continue
            
        current_date = start_date
        while current_date <= datetime.now():
            # Generate readings every 15 minutes for active devices
            timestamp = current_date + timedelta(
                minutes=random.randint(0, 14),
                seconds=random.randint(0, 59)
            )
            
            # Generate sensor value based on device type
            if device['device_type'] == 'Temperature':
                value = round(random.gauss(22, 3), 2)  # ~22°C ± 3°C
                unit = '°C'
            elif device['device_type'] == 'Humidity':
                value = round(random.gauss(50, 10), 1)  # ~50% ± 10%
                unit = '%'
            elif device['device_type'] == 'Pressure':
                value = round(random.gauss(1013, 5), 1)  # ~1013 hPa ± 5
                unit = 'hPa'
            elif device['device_type'] == 'Motion':
                value = random.choice([0, 1])  # Binary
                unit = 'detected'
            elif device['device_type'] == 'Light':
                value = round(random.gauss(300, 100), 1)  # ~300 lux ± 100
                unit = 'lux'
            elif device['device_type'] == 'Air Quality':
                value = round(random.gauss(50, 15), 1)  # AQI ~50 ± 15
                unit = 'AQI'
            else:  # Energy
                value = round(random.gauss(100, 20), 2)  # ~100W ± 20W
                unit = 'W'
            
            reading = {
                'reading_id': reading_id,
                'device_id': device['device_id'],
                'timestamp': timestamp,
                'value': value,
                'unit': unit,
                'quality': random.choices(['Good', 'Fair', 'Poor'], weights=[85, 10, 5])[0],
                'is_anomaly': random.random() < 0.02,  # 2% anomaly rate
                'signal_strength': random.randint(-80, -20),  # dBm
                'battery_level': max(0, device['battery_level'] - random.randint(0, 1)) if device['battery_level'] else None
            }
            
            readings.append(reading)
            reading_id += 1
            current_date += timedelta(minutes=15)
    
    # Generate device events
    events = []
    event_types = ['Boot', 'Shutdown', 'Error', 'Maintenance', 'Battery Low', 'Connection Lost']
    
    for i in range(500):
        device = random.choice(devices)
        event = {
            'event_id': f'E{i+1:08d}',
            'device_id': device['device_id'],
            'event_type': random.choice(event_types),
            'timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
            'severity': random.choices(['Info', 'Warning', 'Error', 'Critical'], weights=[50, 30, 15, 5])[0],
            'message': fake.sentence(nb_words=8),
            'resolved': random.choices([True, False], weights=[80, 20])[0] if random.random() < 0.7 else None,
            'resolved_by': fake.name() if random.random() < 0.3 else None
        }
        events.append(event)
    
    return devices, readings, events


def save_datasets():
    """Save all generated datasets to files."""
    
    # E-commerce data
    print("Generating e-commerce data...")
    customers, products, orders, order_items = generate_ecommerce_data()
    
    pd.DataFrame(customers).to_csv('datasets/ecommerce/customers.csv', index=False)
    pd.DataFrame(products).to_csv('datasets/ecommerce/products.csv', index=False)
    pd.DataFrame(orders).to_csv('datasets/ecommerce/orders.csv', index=False)
    pd.DataFrame(order_items).to_csv('datasets/ecommerce/order_items.csv', index=False)
    
    # Finance data
    print("Generating finance data...")
    accounts, transactions, loans = generate_finance_data()
    
    pd.DataFrame(accounts).to_csv('datasets/finance/accounts.csv', index=False)
    pd.DataFrame(transactions).to_csv('datasets/finance/transactions.csv', index=False)
    pd.DataFrame(loans).to_csv('datasets/finance/loans.csv', index=False)
    
    # IoT data
    print("Generating IoT data...")
    devices, readings, events = generate_iot_data()
    
    pd.DataFrame(devices).to_csv('datasets/iot/devices.csv', index=False)
    pd.DataFrame(readings).to_csv('datasets/iot/sensor_readings.csv', index=False)
    pd.DataFrame(events).to_csv('datasets/iot/device_events.csv', index=False)
    
    # Generate some JSON data as well
    with open('datasets/ecommerce/customers.json', 'w') as f:
        json.dump(customers[:100], f, indent=2, default=str)
    
    with open('datasets/iot/device_status.json', 'w') as f:
        device_status = [
            {
                'device_id': device['device_id'],
                'status': 'online' if device['is_active'] else 'offline',
                'last_seen': fake.date_time_between(start_date='-1d', end_date='now').isoformat(),
                'metrics': {
                    'uptime_hours': random.randint(0, 24*7),
                    'data_points_today': random.randint(0, 96),
                    'error_count': random.randint(0, 5)
                }
            }
            for device in devices[:50]
        ]
        json.dump(device_status, f, indent=2, default=str)
    
    print("Sample data generation complete!")
    print("\nGenerated datasets:")
    print("E-commerce: customers.csv, products.csv, orders.csv, order_items.csv")
    print("Finance: accounts.csv, transactions.csv, loans.csv") 
    print("IoT: devices.csv, sensor_readings.csv, device_events.csv")
    print("\nJSON files: customers.json, device_status.json")


if __name__ == "__main__":
    save_datasets()