FROM postgres

WORKDIR /app

#required or db init will fail
ENV POSTGRES_USER admin
ENV POSTGRES_PASSWORD admin!

RUN mkdir -p /docker-entrypoint-initdb.d/
COPY db/*.sql /docker-entrypoint-initdb.d/


EXPOSE 5432

CMD ["postgres"]