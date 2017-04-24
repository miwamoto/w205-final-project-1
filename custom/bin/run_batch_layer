#!/bin/bash

# Each batch will

# Pull latest weather forecasts (for crime forecasting script)
python /home/ec2-user/pulling_data/openweathermap.py

# Clean/modify new data and upload to clean data to the clean database
python /home/ec2-user/batch_processing/batch.py

# Generate new crime forecasts 
python /home/ec2-user/batch_processing/create_forecasts.py
