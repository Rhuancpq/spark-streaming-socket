FROM openjdk:8-slim
COPY --from=python:3.8 . .

WORKDIR /app

RUN apt-get update && apt-get install net-tools netcat -y

COPY requirements.txt ./requirements.txt

RUN pip3 install -r requirements.txt

COPY . ./

# ENTRYPOINT [ "python3", "./app.py" ]

ENTRYPOINT ["tail", "-F", "/dev/null"]
