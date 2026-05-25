FROM python:3.10
WORKDIR /app
COPY . .
COPY requirements.txt .
RUN pip install -r requirements.txt
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]