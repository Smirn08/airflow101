В начале пайплайна `task_postgres_health_check` - проверка соединения с БД, функция - `posgres_database_check`

Валидация на любую ошибку `psycopg2.OperationalError`
***
В процессе пайплайна на каждом шаге `task_..._sanity_check` - sanity-check проверка на пустые значения во временных файлах csv, функция - `csv_null_data_sanity_check`
***

При отрицательных проверках - сообщения в телегу через оператор `TelegramMessageSenderOperator`

<img src="https://sun9-29.userapi.com/c858436/v858436387/1f9b13/kPWuTdnFX1s.jpg" width="400">

