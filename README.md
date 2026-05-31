# Лабораторная работа N3. Streaming processing с Apache Flink

Проект реализует потоковую обработку исходных CSV-файлов через Kafka и Apache Flink с сохранением результата в PostgreSQL в виде модели данных "звезда".

## Что реализовано

- `producer` на C#/.NET 8 читает 10 CSV-файлов из папки `исходные данные`, преобразует каждую строку в JSON и отправляет сообщение в Kafka topic `sales`.
- Apache Flink читает topic `sales` в streaming-режиме через Flink SQL.
- Flink преобразует поток в DWH-модель:
  - `dim_date`
  - `dim_customer`
  - `dim_seller`
  - `dim_supplier`
  - `dim_store`
  - `dim_product`
  - `fact_sales`
- PostgreSQL хранит итоговые таблицы.
- Docker Compose поднимает PostgreSQL, Kafka, ZooKeeper, Flink JobManager, Flink TaskManager, Flink SQL job и C# producer.

## Структура проекта

- `docker-compose.yml` - вся инфраструктура лабораторной работы.
- `postgres/init.sql` - создание таблиц звезды в PostgreSQL.
- `flink/Dockerfile` - образ Flink с Kafka/JDBC/PostgreSQL connector jars.
- `flink/job.sql` - streaming job на Flink SQL.
- `src/ABDLab3.Producer` - C# producer, который отправляет CSV-строки в Kafka как JSON.
- `исходные данные` - 10 файлов `MOCK_DATA*.csv`.
- `LAB_REPORT.md` - отчет по лабораторной работе.

## Запуск

Из папки `ABDLab3`:

```powershell
docker compose up --build
```

Для чистого перезапуска с удалением старых данных:

```powershell
docker compose down -v --remove-orphans
docker compose up --build
```

После запуска:

- Flink Web UI: `http://localhost:8081`
- Kafka с хоста: `localhost:9092`
- PostgreSQL с хоста:
  - host: `localhost`
  - port: `5433`
  - database: `abd_lab3`
  - user: `postgres`
  - password: `secret`

## Проверка логов

Producer:

```powershell
docker compose logs producer
```

Ожидаемый результат:

```text
ABDLab3 producer started. Files: 10, topic: sales
Sent 1000 messages
...
Sent 10000 messages
ABDLab3 producer finished. Sent messages: 10000
```

Flink SQL client:

```powershell
docker compose logs flink-sql-client
```

Flink job также можно проверить в Web UI на `http://localhost:8081`.

## Проверка данных в PostgreSQL

Количество фактов:

```powershell
docker compose exec postgres psql -U postgres -d abd_lab3 -c "select count(*) from fact_sales;"
```

Количество строк по таблицам:

```powershell
docker compose exec postgres psql -U postgres -d abd_lab3 -c "
select 'dim_date' table_name, count(*) from dim_date
union all select 'dim_customer', count(*) from dim_customer
union all select 'dim_seller', count(*) from dim_seller
union all select 'dim_supplier', count(*) from dim_supplier
union all select 'dim_store', count(*) from dim_store
union all select 'dim_product', count(*) from dim_product
union all select 'fact_sales', count(*) from fact_sales;"
```

Пример аналитического запроса:

```powershell
docker compose exec postgres psql -U postgres -d abd_lab3 -c "
select
  p.category,
  sum(f.sale_quantity) as total_quantity,
  round(sum(f.sale_total_price), 2) as revenue
from fact_sales f
join dim_product p on p.product_id = f.product_id
group by p.category
order by revenue desc
limit 10;"
```

## Примечание по C# и Flink

Официального стабильного C# API для Apache Flink нет. Поэтому в работе C# используется для приложения-источника данных, а streaming job описан на Flink SQL. Flink SQL является штатным способом разработки Flink-приложений: он создает Kafka source, выполняет потоковые преобразования и пишет результат в PostgreSQL через JDBC sink.
