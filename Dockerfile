FROM quay.io/astronomer/astro-runtime:11.3.0

WORKDIR /app

COPY requirements_ex.txt /app/requirements_ex.txt


RUN pip install --no-cache-dir -r /app/requirements_ex.txt

COPY dags/module/example_train.py /app/example_train.py

CMD ["python", "/app/example_train.py"]
