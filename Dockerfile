FROM python:3.8

ADD main.py .
ADD cert_kambi_prod.pem .
ADD private_key_kambi_prod.pem .
ADD keyfile.json .

COPY requirements.txt ./
RUN pip install -r requirements.txt
EXPOSE 8080

CMD ["python", "./main.py"]
