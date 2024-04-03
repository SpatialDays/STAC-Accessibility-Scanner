# Use an official Python runtime as the base image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port that the application listens on
EXPOSE 5000

# Run the application
CMD ["gunicorn", "server:app", "-b", "0.0.0.0:8000", "-w", "4"]
