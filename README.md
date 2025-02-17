Data Cleaner Script Explanation
This guide explains a comprehensive data cleaning script that handles various common data preparation tasks. The script is designed to work with multiple file formats and implements several best practices for data cleaning.
Let's break down each component and understand how it works.
1. Required Libraries
First, let's import all the necessary libraries:
```python
import pandas as pd
import numpy as np
import time
import os
import random
from pathlib import Path
```
2. Utility Functions
2.1 Random Delay Function
This function adds a random delay between operations, which can be useful for user experience and resource management:
```python
def random_delay(min_sec=1, max_sec=4):
    """Generates a random delay between min_sec and max_sec."""
    delay = random.randint(min_sec, max_sec)
    print(f'Please wait {delay} seconds...')
    time.sleep(delay)
```
2.2 Dataset Loading Function
This function handles multiple file formats (CSV, Excel, JSON, Parquet) and loads them appropriately:
```python
def load_dataset(data_path):
    """Loads a dataset based on its file type."""
    file_extension = Path(data_path).suffix.lower()
    if file_extension == '.csv':
        return pd.read_csv(data_path, encoding_errors='ignore')
    elif file_extension in ('.xlsx', '.xls'):
        return pd.read_excel(data_path)
    elif file_extension == '.json':
        return pd.read_json(data_path)
    elif file_extension == '.parquet':
        return pd.read_parquet(data_path)
    else:
        raise ValueError('Unsupported file type. Please provide a CSV, Excel, JSON, or Parquet file.')
```
3. Data Cleaning Functions
3.1 Handling Duplicates
This function identifies and removes duplicate records while saving them for reference:
```python
def handle_duplicates(data, data_name):
    """Handles duplicate records in the dataset."""
    total_duplicates = data.duplicated().sum()
    
    if total_duplicates > 0:
        # Save duplicates for reference
        duplicate_records = data[data.duplicated()]
        duplicate_records.to_csv(f'{data_name}_duplicates.csv', index=False)
        # Remove duplicates
        data = data.drop_duplicates()
    return data
```
3.2 Handling Missing Values
This function deals with missing values differently based on the column type:
```python
def handle_missing_values(data):
    """Handles missing values in the dataset."""
    for col in data.columns:
        if data[col].dtype in (float, int):
            # Fill numeric columns with mean
            data[col] = data[col].fillna(data[col].mean())
        else:
            # Drop rows with missing non-numeric values
            data.dropna(subset=[col], inplace=True)
    return data
```
3.3 Dropping Unnecessary Columns
This function removes columns with excessive missing values:
```python
def drop_unnecessary_columns(data):
    """Drops columns with more than 50% missing values."""
    columns_to_drop = [col for col in data.columns if data[col].isnull().mean() > 0.5]
    if columns_to_drop:
        data.drop(columns=columns_to_drop, inplace=True)
    return data
```
3.4 Standardizing Column Names
This function ensures consistent column naming conventions:
```python
def standardize_columns(data):
    """Standardizes column names by removing spaces and special characters."""
    data.columns = [col.strip().replace(' ', '_').lower() for col in data.columns]
    return data
```
3.5 Handling Outliers
This function identifies and handles outliers using the Interquartile Range (IQR) method:
```python
def handle_outliers(data):
    """Handles outliers in numeric columns by capping them to IQR bounds."""
    for col in data.select_dtypes(include=[np.number]).columns:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        data[col] = np.where(
            (data[col] < lower_bound) | (data[col] > upper_bound),
            np.nan,
            data[col]
        )
    return data
```
4. Main Cleaning Function
The data_cleaner function orchestrates the entire cleaning process:
```python
def data_cleaner(data_path, data_name):
    """Main function to clean the dataset."""
    # Validate file path
    if not os.path.exists(data_path):
        print('Incorrect path. Please enter a valid path.')
        return

    # Load dataset
    data = load_dataset(data_path)
    
    # Apply cleaning steps
    data = handle_duplicates(data, data_name)
    data = handle_missing_values(data)
    data = drop_unnecessary_columns(data)
    data = standardize_columns(data)
    data = handle_outliers(data)
    
    # Save cleaned dataset
    cleaned_file_path = f'{data_name}_clean_data.csv'
    data.to_csv(cleaned_file_path, index=False)
    
    return data
```
5. Example Usage
Here's how you can use the data cleaner:
```python
# Example usage
data_path = 'path/to/your/dataset.csv'  # Replace with your dataset path
data_name = 'my_dataset'

cleaned_data = data_cleaner(data_path, data_name)
```

Key Features of the Data Cleaner

The data cleaner includes several powerful features:

Flexible File Format Support

Handles CSV, Excel, JSON, and Parquet files
Automatic format detection using file extension

Comprehensive Cleaning Steps

Duplicate record handling with backup
Intelligent missing value treatment
Removal of columns with excessive missing data
Column name standardization
Outlier detection and handling using IQR method

User-Friendly Features

Progress updates throughout the cleaning process
Automatic saving of cleaned dataset
Preservation of duplicate records for reference

Error Handling

File path validation
File format validation
Graceful handling of various data types

This script provides a robust foundation for data cleaning tasks and can be easily extended with additional functionality as needed.








