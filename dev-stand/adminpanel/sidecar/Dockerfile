# Patroni-эмулятор стенда: python + pg8000 (как сайдкар ../pg; arch/04 §2.3)
FROM python:3.12-alpine
RUN pip install --no-cache-dir pg8000
COPY emulator.py /emulator.py
EXPOSE 8008
CMD ["python", "/emulator.py"]
