# SoftFlow Booking Bot

Надёжный Telegram-бот для онлайн-записи одного мастера услуг. Эта папка —
отдельная переработанная копия исходного massageBot и не заменяет исходный ZIP.

## Готовые функции

- услуги, стоимость и любая длительность;
- недельное расписание мастера;
- индивидуальные часы, перерывы и выходные на конкретную дату;
- только реально свободные окна;
- минимальное время до записи;
- клиентская запись с повторной проверкой слота;
- просмотр собственных будущих записей;
- перенос и отмена клиентом;
- ручная запись мастером после звонка или переписки;
- отмена мастером с уведомлением клиента;
- уведомления мастеру о новой, перенесённой и отменённой записи;
- несколько напоминаний клиенту перед визитом;
- SQLite с транзакционной защитой от пересечений;
- автоматический перезапуск процесса через systemd;
- безопасные секреты в переменных окружения;
- структура таблиц для будущего листа ожидания и нескольких услуг.

Лист ожидания, уведомления об освободившемся окне и выбор нескольких услуг
сейчас намеренно не включены в интерфейс.

## Структура

| Путь | Назначение |
|---|---|
| app.py | Запуск, подключение модулей и общий перехват ошибок |
| config.py | Загрузка секретов и настроек |
| settings/master.json | Контакты, расписание, перерывы, тексты и параметры |
| settings/services.json | Услуги, стоимость и длительность |
| bot/client.py | Сценарии клиента |
| bot/master.py | Панель мастера и ручная запись |
| bot/booking.py | Расчёт расписания и свободных окон |
| bot/storage.py | SQLite, транзакции и защита от пересечений |
| bot/notifications.py | Уведомления и напоминания |
| bot/ui.py | Кнопки и опциональные line-иконки |
| tests/ | Автоматические проверки логики |

## Адаптация под нового мастера

До первого запуска изменить только:

1. settings/master.json — название, контакты, расписание и напоминания;
2. settings/services.json — услуги, стоимость и длительность;
3. .env — токен и Telegram ID владельца.

Саму логику бота для мастера ресниц переписывать не потребуется. Если база уже
создана, услуги в ней не заменяются автоматически: это защищает действующие
записи. Для нового клиента используется новая пустая база.

## Кнопки

Интерфейс использует чистые текстовые кнопки без пёстрых смайликов. Telegram
стили выделяют подтверждение, основные действия и отмену. При наличии
разрешённых custom emoji их ID можно добавить в settings/master.json. Пустые
значения безопасно включают текстовый вариант.

## Локальная проверка

Требуется Python 3.11 или новее.

    python -m venv .venv
    .venv/bin/pip install -r requirements.txt
    python -m unittest discover -v

Создать .env по примеру .env.example и запустить:

    export BOT_TOKEN="..."
    export MASTER_ID="..."
    python app.py

## Бесплатное размещение на Oracle Cloud Always Free

Подходит небольшой Linux-инстанс Ubuntu. Общий порядок:

1. Создать Always Free Linux VM.
2. Разрешить только SSH во входящих правилах. Боту не требуется открытый
   HTTP-порт, потому что он использует Telegram long polling.
3. Подключиться по SSH и установить Python:

       sudo apt update
       sudo apt install -y python3 python3-venv

4. Создать системного пользователя и папку:

       sudo useradd --system --home /opt/softflow-booking --shell /usr/sbin/nologin softflowbot
       sudo mkdir -p /opt/softflow-booking
       sudo chown -R softflowbot:softflowbot /opt/softflow-booking

5. Загрузить содержимое этой папки в /opt/softflow-booking.
6. Создать виртуальное окружение и установить зависимости:

       sudo -u softflowbot python3 -m venv /opt/softflow-booking/.venv
       sudo -u softflowbot /opt/softflow-booking/.venv/bin/pip install -r /opt/softflow-booking/requirements.txt

7. Создать /opt/softflow-booking/.env:

       BOT_TOKEN=токен_бота
       MASTER_ID=telegram_id_мастера
       DATABASE_PATH=/opt/softflow-booking/data/bot.db
       SETTINGS_PATH=/opt/softflow-booking/settings/master.json

8. Ограничить доступ к секретам:

       sudo chown softflowbot:softflowbot /opt/softflow-booking/.env
       sudo chmod 600 /opt/softflow-booking/.env

9. Установить deploy/softflow-booking.service:

       sudo cp /opt/softflow-booking/deploy/softflow-booking.service /etc/systemd/system/
       sudo systemctl daemon-reload
       sudo systemctl enable --now softflow-booking

10. Проверить:

       sudo systemctl status softflow-booking
       sudo journalctl -u softflow-booking -n 100 --no-pager

## Подключение к сайту

После запуска добавить на SoftFlow ссылку:

    https://t.me/ИМЯ_ТЕСТОВОГО_БОТА

или кнопку:

    <a href="https://t.me/ИМЯ_ТЕСТОВОГО_БОТА" target="_blank" rel="noopener">
      Открыть демо записи
    </a>

## Резервная копия

Безопасная копия работающей SQLite-базы:

    .venv/bin/python scripts/backup_database.py

## Production check

- BOT_TOKEN и MASTER_ID отсутствуют в GitHub;
- команда python -m unittest discover -v проходит без ошибок;
- услуги и цены соответствуют демо;
- часовой пояс — Europe/Minsk;
- рабочие часы, перерыв и выходной отображаются правильно;
- два клиента не могут занять пересекающиеся интервалы;
- ручная запись мастера сразу убирает окно;
- клиент видит только собственные записи;
- перенос проверяет занятость повторно;
- отмена освобождает время;
- клиент и мастер получают нужные уведомления;
- тестовые напоминания проверены с коротким интервалом;
- systemd перезапускает процесс после сбоя;
- резервная копия базы создаётся.
