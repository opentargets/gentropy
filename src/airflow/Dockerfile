FROM apache/airflow:2.7.1-python3.10

# Install additional Python requirements.
# --no-cache-dir is a good practice when installing packages using pip, because it helps to keep the image lightweight.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user --quiet --upgrade pip
RUN pip install --no-cache-dir --user --quiet --requirement /requirements.txt
