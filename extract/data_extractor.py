"""
ELT Pipeline - EXTRACT Phase
Data Extractor for E-commerce Sales Data

This module handles the EXTRACT step of the ELT pipeline:
- Extracts data from various sources (APIs, databases, files)
- Outputs raw data as CSV/JSON for loading into Snowflake
"""

import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import os

class DataExtractor:
    """
    Extracts data from source systems.
    In ELT, we extract raw data WITHOUT transformation.
    """
    
    def __init__(self, output_dir='./data/raw'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.extraction_timestamp = datetime.now()
    
    def extract_orders(self, source_path=None):
        """
        Extract orders data from source.
        Returns raw data as-is (no transformation in ELT Extract phase)
        """
        # Sample e-commerce orders data
        orders_data = [
            {'order_id': 1001, 'customer_id': 'C001', 'order_date': '2024-01-15', 'total_amount': 299.99, 'status': 'completed'},
            {'order_id': 1002, 'customer_id': 'C002', 'order_date': '2024-01-15', 'total_amount': 149.50, 'status': 'completed'},
            {'order_id': 1003, 'customer_id': 'C001', 'order_date': '2024-01-16', 'total_amount': 599.00, 'status': 'pending'},
            {'order_id': 1004, 'customer_id': 'C003', 'order_date': '2024-01-16', 'total_amount': 89.99, 'status': 'completed'},
            {'order_id': 1005, 'customer_id': 'C004', 'order_date': '2024-01-17', 'total_amount': 1299.00, 'status': 'shipped'},
        ]
        
        df = pd.DataFrame(orders_data)
        output_file = f"{self.output_dir}/raw_orders.csv"
        df.to_csv(output_file, index=False)
        print(f"Extracted {len(df)} orders to {output_file}")
        return df
    
    def extract_customers(self):
        """Extract customer data"""
        customers_data = [
            {'customer_id': 'C001', 'name': 'John Smith', 'email': 'john@email.com', 'created_at': '2023-06-01'},
            {'customer_id': 'C002', 'name': 'Jane Doe', 'email': 'jane@email.com', 'created_at': '2023-07-15'},
            {'customer_id': 'C003', 'name': 'Bob Wilson', 'email': 'bob@email.com', 'created_at': '2023-08-20'},
            {'customer_id': 'C004', 'name': 'Alice Brown', 'email': 'alice@email.com', 'created_at': '2023-09-10'},
        ]
        
        df = pd.DataFrame(customers_data)
        output_file = f"{self.output_dir}/raw_customers.csv"
        df.to_csv(output_file, index=False)
        print(f"Extracted {len(df)} customers to {output_file}")
        return df
    
    def extract_products(self):
        """Extract product catalog data"""
        products_data = [
            {'product_id': 'P001', 'name': 'Laptop Pro', 'category': 'Electronics', 'price': 999.99},
            {'product_id': 'P002', 'name': 'Wireless Mouse', 'category': 'Electronics', 'price': 49.99},
            {'product_id': 'P003', 'name': 'Office Chair', 'category': 'Furniture', 'price': 299.00},
            {'product_id': 'P004', 'name': 'Desk Lamp', 'category': 'Home', 'price': 39.99},
        ]
        
        df = pd.DataFrame(products_data)
        output_file = f"{self.output_dir}/raw_products.csv"
        df.to_csv(output_file, index=False)
        print(f"Extracted {len(df)} products to {output_file}")
        return df
    
    def run_full_extraction(self):
        """Run complete extraction for all data sources"""
        print(f"Starting ELT Extract Phase at {self.extraction_timestamp}")
        print("="*50)
        
        self.extract_orders()
        self.extract_customers()
        self.extract_products()
        
        print("="*50)
        print("EXTRACT phase completed. Raw files ready for LOAD phase.")


if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.run_full_extraction()
