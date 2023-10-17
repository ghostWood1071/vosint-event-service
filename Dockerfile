FROM python:3.10-bullseye

WORKDIR /usr/app


COPY requirements.txt .
RUN pip install -r requirements.txt 

COPY . .
EXPOSE 6101
CMD [ "python","events.py"]