FROM python:3.12-alpine
RUN pip install --no-cache-dir pg8000
COPY rolecheck.py /rolecheck.py
EXPOSE 8008
CMD ["python", "/rolecheck.py"]
