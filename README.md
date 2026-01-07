# DonationSync

A PySpark-based donation processing system that aggregates donation data from CSV files and maintains a consolidated lifetime giving table.

## Overview

DonationSync processes CSV files containing donation records, aggregates them by donor, and maintains a `DonorLifetimeGiving` table that tracks:
- **DonorID**: Unique identifier for each donor
- **Name**: Donor's name
- **LifetimeAmount**: Sum of all donations for the donor
- **LastDonation**: Most recent donation date

The system automatically merges new donation data with existing records, updating lifetime amounts and last donation dates as needed.

## Requirements

- **Python**: 3.11 (required for PySpark 3.5.1 compatibility)
- **Java**: Java 8 or later (required by Spark)
- **Operating System**: Windows (configured with Windows-specific workarounds)

## Installation

1. **Install Python 3.11** (if not already installed)
   
   **PowerShell:**
   ```powershell
   py --list
   ```
   
   **CMD:**
   ```cmd
   py --list
   ```

2. **Create a virtual environment**
   
   **PowerShell:**
   ```powershell
   py -3.11 -m venv venv
   ```
   
   **CMD:**
   ```cmd
   py -3.11 -m venv venv
   ```

3. **Activate the virtual environment**
   
   **PowerShell:**
   ```powershell
   .\venv\Scripts\Activate.ps1
   ```
   
   **CMD:**
   ```cmd
   venv\Scripts\activate.bat
   ```

4. **Install dependencies**
   
   **PowerShell:**
   ```powershell
   pip install -r requirements.txt
   ```
   
   **CMD:**
   ```cmd
   pip install -r requirements.txt
   ```

## Project Structure

```
DonationSync/
├── Incoming/              # Place CSV files here for processing
├── Processed/             # Processed files are moved here (if implemented)
├── spark-warehouse/       # Spark data warehouse (contains processed Parquet files)
│   └── donorlifetimegiving/
│       └── data.parquet   # The DonorLifetimeGiving table
├── hadoop/                # Windows workaround directory
│   └── bin/
│       └── winutils.exe   # Hadoop utility for Windows
├── process_incoming_donations.py  # Main processing script
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## CSV File Format

Place CSV files in the `Incoming` folder with the following format:

```csv
DonorID,Name,Amount,Date
1,Joe Smith,5,1/2/2026
2,Jane Smith,10,1/3/2026
3,Bob Johnson,25,1/4/2026
```

**Required Columns:**
- `DonorID`: Integer identifier for the donor
- `Name`: Donor's name (string)
- `Amount`: Donation amount (numeric)
- `Date`: Donation date in `M/d/yyyy` format (e.g., `1/2/2026`)

## Usage

### Basic Usage

1. **Place CSV files in the `Incoming` folder**

2. **Run the processing script**
   
   **PowerShell:**
   ```powershell
   python process_incoming_donations.py
   ```
   
   **CMD:**
   ```cmd
   python process_incoming_donations.py
   ```

The script will:
- Read all CSV files from the `Incoming` folder
- Aggregate donations by `DonorID`
- Merge with existing `DonorLifetimeGiving` table (if it exists)
- Display the updated table contents








