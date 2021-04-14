FROM python:3.8

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ .
COPY textract/ ./mock-textract

CMD ["python", "pipeline.py"]