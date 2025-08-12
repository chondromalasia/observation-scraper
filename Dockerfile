FROM python:3.11.4-slim

WORKDIR /app

COPY setup.py setup.cfg requirements.txt ./
COPY src ./src/

RUN pip install -e .

CMD ["tail", "-f", "/dev/null"]
