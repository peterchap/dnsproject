#!/bin/bash

# Source the conda environment
source /root/dnsenv/bin/activate

# Run the Python script
python3 /root/dnsproject/download_daily.py

# Check if script1.py executed successfully
if [ $? -eq 0 ]; then
    echo "Daily download completed successfully. Running DNS lookup..."
    # Run the second Python script
    python3 /root/dnsproject/asyncio_dns_daily_updates.py
else
    echo "script1.py failed. script2.py will not be run."
    exit 1  # Exit with an error status
fi
