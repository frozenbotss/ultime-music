# Use an official Python 3.10 slim image as the base
FROM python:3.10-slim

# Update apt and install ffmpeg and git, then clean up cache
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg git && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container to /app
WORKDIR /app

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project into the container
COPY . .

# (Optional) Expose the port your API listens on
EXPOSE 8080

# Define the command to run your API (adjust if your entry point is different)
CMD ["python", "bot.py"]
