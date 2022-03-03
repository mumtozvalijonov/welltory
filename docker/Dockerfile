FROM python:3.8 as base

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt


FROM base as web
CMD ["uvicorn", "main:app", "--env-file", ".env", "--host 0.0.0.0"]

FROM base as calculator
CMD ["python", "calculator.py"]
