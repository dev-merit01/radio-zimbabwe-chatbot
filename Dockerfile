FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
# Copy only server files: local databases and credentials never enter the image.
COPY manage.py ./
COPY radio_zimbabwe ./radio_zimbabwe
COPY apps ./apps
COPY templates ./templates
COPY static ./static
COPY deploy ./deploy
RUN DJANGO_SECRET_KEY=build-only-not-a-runtime-secret python manage.py collectstatic --noinput \
    && useradd --create-home airvote && chown -R airvote:airvote /app
USER airvote
CMD ["sh", "deploy/web.sh"]
