# Тестовый перенос 1С PDM → RabbitMQ → 1С ERP

Репозиторий содержит одноразовый Windows Worker для переноса видов номенклатуры, затем номенклатуры, из PDM в ERP через RabbitMQ. Worker читает данные PDM через существующий COM-модуль _кадИнтеграцияВызов, публикует устойчивые сообщения и подтверждает их в RabbitMQ только после HTTP-ответа ERP с success=true.

## Безопасность

- Worker требует ERP_TARGET=TEST и завершается до подключения к PDM/RabbitMQ, если флаг не задан.
- ERP_ENDPOINT задаётся явно. Перед запуском вручную проверьте, что URL ведёт именно в тестовую базу ERP.
- Пароли не хранятся в репозитории; задаются переменными окружения.
- Worker не изменяет PDM: экспорт выполняется только чтением.
- Очередь и DLQ имеют отдельные тестовые имена.
- Сообщения с ошибкой ERP отправляются в DLQ и не подтверждаются как успешно обработанные.
- Идентификатор сообщения детерминирован от типа события, источника и ссылки. Повторный запуск использует тот же MessageId.

## Что подготовить в 1С

1. В PDM загрузить и обновить расширение из ветки feature/test-pdm-erp-transfer репозитория 1C_PDM_Enhanced. Убедиться, что общий модуль _кадИнтеграцияВызов доступен через внешнее соединение.
2. В ERP загрузить и обновить расширение из ветки feature/test-pdm-erp-transfer репозитория 1C_ERP_Enhanced.
3. Опубликовать HTTP-сервис СоздатьИзделие на тестовой базе. ERP_ENDPOINT должен содержать полный адрес HTTP-шаблона /items. Путь публикации и имя базы на сервере необходимо проверить на месте.
4. Убедиться, что в тестовой ERP существуют нужные единицы измерения. Автоматическое создание единиц не выполняется.
5. Настроить Basic Authentication для HTTP-сервиса и права пользователя на чтение/создание видов номенклатуры, номенклатуры и запись регистра Расш1_ОбработанныеСообщения.

## Сборка

На Windows с установленным .NET SDK 9 и 64-разрядной платформой 1С PDM:

    dotnet restore .\src\RabbitMqPdmErpWorker\RabbitMqPdmErpWorker.csproj
    dotnet build .\src\RabbitMqPdmErpWorker\RabbitMqPdmErpWorker.csproj -c Release

## Переменные окружения PowerShell

Задайте реальные тестовые параметры в текущем окне PowerShell. Пароли не вводите в файл и не коммитьте:

    $env:ERP_TARGET = 'TEST'
    $env:PDM_CONNECTION_STRING = 'Srvr="192.168.50.173";Ref="1C_pdm";Usr="Администратор";Pwd="<PDM_PASSWORD>";'
    $env:ERP_ENDPOINT = 'http://<ERP_TEST_HOST>/<ERP_TEST_PUBLICATION>/hs/СоздатьИзделие/items'
    $env:ERP_USER = '<ERP_HTTP_USER>'
    $env:ERP_PASSWORD = '<ERP_HTTP_PASSWORD>'
    $env:RABBITMQ_HOST = '192.168.50.173'
    $env:RABBITMQ_PORT = '5672'
    $env:RABBITMQ_VHOST = '/'
    $env:RABBITMQ_USER = '<RABBITMQ_USER>'
    $env:RABBITMQ_PASSWORD = '<RABBITMQ_PASSWORD>'

PDM_CONNECTION_STRING должен использовать зарегистрированный на машине запуска V83.COMConnector платформы PDM 8.3.12.1529. Не меняйте регистрацию COM на платформу ERP.

## Одноразовый запуск

    dotnet run --project .\src\RabbitMqPdmErpWorker\RabbitMqPdmErpWorker.csproj -c Release

Worker выгружает снимок, публикует сначала все виды, затем все позиции и ждёт ответ ERP на каждое сообщение. Код завершения 0 означает, что каждый ответ ERP содержал HTTP 2xx и success=true. При RESULT=FAILED анализируйте сообщение и DLQ до повторного запуска.

## Контракт сообщений

Вид номенклатуры:

    {
      "schemaVersion": 1,
      "messageId": "<стабильный UUID>",
      "eventType": "nomenclatureView.created",
      "sourceSystem": "1C_PDM",
      "sourceRef": "<UUID вида в PDM>",
      "itemName": "<точное наименование вида>",
      "nomenclatureType": "Товар",
      "storageUnit": "<представление единицы>",
      "usesCharacteristics": false
    }

Номенклатура:

    {
      "schemaVersion": 1,
      "messageId": "<стабильный UUID>",
      "eventType": "nomenclature.created",
      "sourceSystem": "1C_PDM",
      "sourceRef": "<UUID позиции в PDM>",
      "itemName": "<точное наименование позиции>",
      "nomenclatureView": "<точное наименование вида>",
      "nomenclatureViewSourceRef": "<UUID вида в PDM>",
      "nomenclatureType": "Товар",
      "storageUnit": "<представление единицы>",
      "usesCharacteristics": false
    }

## Ограничения текущего тестового приёмника

- Тестовый приёмник ERP поддерживает тип Товар для видов и позиций. Другие типы отклоняются, а не преобразуются молча.
- Виды с включёнными характеристиками отклоняются.
- Если единица не найдена в ERP или точное имя уже связано с другими реквизитами, сообщение отклоняется.
- Это одноразовый пакетный запуск, не Windows-служба и не непрерывный синхронизатор.
- Пакет включает все не удалённые виды и позиции из PDM. Перед первым запуском проверьте размер выгрузки.
- Автоматическое обновление уже созданных объектов не выполняется.
- Загрузку/публикацию расширений и проверку фактической записи в вашей тестовой базе нужно выполнить на сервере 1С. GitHub не позволяет подтвердить компиляцию BSL или состояние конкретной базы.
