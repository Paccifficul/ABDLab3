# Отчет по лабораторной работе N3

## Тема

Анализ больших данных. Streaming processing с Apache Flink: чтение JSON-сообщений из Kafka, преобразование потока в модель данных "звезда" и запись результата в PostgreSQL.

## Цель работы

Получить практический опыт построения потокового пайплайна обработки данных. В рамках работы необходимо эмулировать источник данных, отправить строки CSV-файлов в Kafka в формате JSON, обработать поток во Flink и сохранить преобразованные данные в PostgreSQL.

## Используемые технологии

- C# / .NET 8 - приложение producer для чтения CSV и отправки сообщений в Kafka.
- Apache Kafka - брокер сообщений для входного потока.
- Apache Flink 1.19 - потоковая обработка данных.
- Flink SQL - описание streaming job.
- PostgreSQL 17 - хранилище DWH-модели.
- Docker Compose - запуск инфраструктуры.

## Исходные данные

В папке `исходные данные` размещены 10 CSV-файлов `MOCK_DATA*.csv`. Каждый файл содержит 1000 строк. Каждая строка описывает продажу товара: данные покупателя, продавца, продукта, магазина, поставщика и самой продажи.

Producer добавляет к каждой записи технические поля:

- `source_file` - имя исходного CSV-файла;
- `source_row_number` - номер строки внутри файла.

Эти поля используются для формирования уникального ключа факта продажи.

## Архитектура решения

1. Контейнер `postgres` поднимает PostgreSQL и выполняет `postgres/init.sql`, создавая таблицы модели "звезда".
2. Контейнеры `zookeeper` и `kafka` поднимают Kafka.
3. Контейнеры `jobmanager` и `taskmanager` поднимают Apache Flink.
4. Контейнер `flink-sql-client` отправляет во Flink SQL job из файла `flink/job.sql`.
5. Контейнер `producer` запускает C# приложение:
   - читает CSV;
   - преобразует строки в JSON;
   - отправляет сообщения в Kafka topic `sales`.
6. Flink читает Kafka topic `sales`, нормализует поток и пишет данные в PostgreSQL через JDBC sink.

## Модель данных

В PostgreSQL создаются следующие таблицы:

- `dim_date` - календарное измерение;
- `dim_customer` - покупатели и данные их питомцев;
- `dim_seller` - продавцы;
- `dim_supplier` - поставщики;
- `dim_store` - магазины;
- `dim_product` - товары;
- `fact_sales` - факты продаж.

Для `dim_customer`, `dim_seller` и `dim_product` используются идентификаторы из исходных данных. Для `dim_store` и `dim_supplier` формируются стабильные текстовые ключи на основе значимых атрибутов. Факт продажи получает ключ вида `source_file:source_row_number`, что делает его уникальным для всех 10 файлов.

## Потоковая обработка во Flink

Flink SQL job содержит:

- Kafka source table `kafka_sales`;
- view `normalized_sales`, где рассчитываются ключи измерений и приводятся даты;
- JDBC sink tables для таблиц PostgreSQL;
- `EXECUTE STATEMENT SET`, который запускает несколько потоковых вставок одновременно.

Потоковые вставки наполняют измерения уникальными значениями и записывают все события продаж в `fact_sales`.

## Запуск

Из папки `ABDLab3`:

```powershell
docker compose up --build
```

Для чистого запуска:

```powershell
docker compose down -v --remove-orphans
docker compose up --build
```

## Проверка результата

Проверка количества фактов:

```powershell
docker compose exec postgres psql -U postgres -d abd_lab3 -c "select count(*) from fact_sales;"
```

Ожидаемое значение после полной отправки данных - `10000`.

Проверка заполнения всех таблиц:

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

## Вывод

В ходе лабораторной работы реализован потоковый пайплайн обработки данных. C# приложение эмулирует внешний источник и отправляет CSV-строки в Kafka в формате JSON. Apache Flink в streaming-режиме читает сообщения, преобразует их в DWH-модель "звезда" и сохраняет результат в PostgreSQL. Решение полностью запускается через Docker Compose и может быть проверено по логам producer, Flink Web UI и SQL-запросам к PostgreSQL.
