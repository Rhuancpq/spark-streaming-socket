FROM datamechanics/spark:3.2.1-hadoop-3.3.1-java-8-scala-2.12-python-3.8-latest

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

WORKDIR /opt/application/

COPY requirements.txt ./requirements.txt

RUN pip3 install -r requirements.txt

COPY . ./

# ENTRYPOINT [ "python3", "./app.py" ]

# ENTRYPOINT ["tail", "-F", "/dev/null"]

CMD ["driver", "local:///opt/application/app.py" ]