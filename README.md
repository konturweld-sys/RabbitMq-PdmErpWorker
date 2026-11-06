# ЕКК — единое корпоративное ядро

Рабочий процесс ЕКК на .NET 10: RPC через RabbitMQ, транзакционная регистрация в SQL Server и публикация событий через transactional Outbox.

## Операции

- **RegisterEntity** — создаёт Entity с UUIDv7; EntityTypeCode разрешается только по активной записи ekk.EntityType. Начальный статус — RegistrationPending.
- **RegisterSourceObject** — связывает локальный объект с Entity по уникальной идентичности (SourceSystemInstanceKey, ExternalObjectType, ExternalObjectId).
- **CompleteEntityRegistration** — переводит Entity из RegistrationPending в Registered или RegistrationError и одновременно записывает историю.

Все операции используют RequestIdempotency. Повтор с тем же ключом и тем же содержимым возвращает сохранённый ответ; повторное использование ключа с другим содержимым отклоняется.

## Транзакции и доставка

При регистрации Entity или SourceObject в одной SQL-транзакции выполняется бизнес-изменение, увеличивается Entity.LastEventSequence, создаётся ekk.OutboxMessage, сохраняется ответ идемпотентности и выполняется COMMIT.

Outbox Publisher использует persistent delivery, mandatory=true и RabbitMQ Publisher Confirms. Статус Published устанавливается только после подтверждения брокера и отсутствия Basic.Return. При ошибке увеличивается AttemptCount, задаётся NextAttemptAt с ограниченной экспоненциальной задержкой и сохраняется LastError. Сбой после подтверждения RabbitMQ, но до обновления SQL может вызвать повторную публикацию; потребитель должен дедуплицировать по MessageId (at-least-once).

**Важное правило маршрутизации RabbitMQ:** mandatory=true обнаруживает сообщение, которое не попало ни в одну очередь. Если на момент публикации привязана только очередь PDM, брокер успешно доставит сообщение в PDM и подтвердит публикацию. Добавление привязки ERP позднее не копирует ранее опубликованное сообщение в очередь ERP. Все целевые очереди должны быть привязаны до публикации события.

Входящий RPC-запрос ACK-ается только после обработки и подтверждённой публикации ответа. Ошибка SQL или RabbitMQ приводит к NACK/requeue. Обработка последовательная, без конкурентного доступа к RabbitMQ IModel.

## RabbitMQ

- Request queue: ekk.requests.
- RPC response exchange: ekk.responses.
- Outbox event exchange: ekk.events.
- RPC routing key: response.{SystemCode}.{InstanceCode}.{MessageType}.
- Event routing keys: entity.created и entity.source-object-registered.
- Конверт RPC: MessageId, CorrelationId, MessageType, SchemaVersion=1, CreatedAt, Payload.

Очереди и exchange должны быть созданы заранее; Worker не переобъявляет топологию.

Для тестового контура используются:
- RabbitMQ: 192.168.50.173:5672;
- vhost: corporate_test;
- PDM: SourceSystemInstanceKey=1, SystemCode=PDM, InstanceCode=PDM_TEST;
- ERP: SourceSystemInstanceKey=2, SystemCode=ERP, InstanceCode=ERP_TEST.

## Соответствие фактической схеме БД

Worker не выполняет DDL, миграции или заполнение справочников. Он использует существующие таблицы и поля:

- ekk.Entity;
- ekk.EntityType;
- ekk.SourceObject;
- ekk.RequestIdempotency;
- ekk.EntityRegistrationHistory;
- ekk.OutboxMessage, включая NextAttemptAt и LastError.

В фактической ekk.SourceObject нет столбца Namespace; применяется существующий уникальный ключ без Namespace. Добавление Namespace требует отдельной согласованной миграции.

EntityType, SourceSystem и SourceSystemInstance должны быть заполнены согласованными данными до запуска. Приложение не генерирует коды типов и тестовые записи. Владелец Entity определяется по Entity.CreatedBySourceSystemInstanceKey. EntityTypeOwner и SourceSystemInstancePermission в текущей БД отсутствуют и не используются.

Outbox Publisher рассчитан на один экземпляр на одну БД; для нескольких экземпляров нужен согласованный механизм захвата строк Outbox.

Текущая ЕКК отвечает за идентичность Entity и SourceObject. Она не копирует полный состав реквизитов номенклатуры между базами 1С: данные номенклатуры передают адаптеры PDM и ERP через отдельный прикладной контракт.

## Конфигурация

Секреты не хранятся в репозитории. Задайте переменные окружения:

- Sql__ConnectionString
- RabbitMq__UserName
- RabbitMq__Password
- Ekk__SourceSystemInstanceKey
- Ekk__SystemCode
- Ekk__InstanceCode

SourceSystemInstanceKey задаётся конфигурацией установленного экземпляра и не принимается из сообщения.

В appsettings.json уже указаны тестовые адрес RabbitMQ и vhost corporate_test. Строка подключения SQL Server и секрет RabbitMQ передаются только через переменные окружения.

## Сборка и запуск

GitHub Actions выполняет restore и Release build, затем публикует самодостаточный Windows x64 пакет. .NET Runtime на сервере для запуска такого пакета не требуется.

1. Откройте вкладку Actions репозитория и выберите последний успешный запуск Build.
2. Скачайте артефакт EKK-win-x64-<commit SHA> и распакуйте его на сервере ЕКК.
3. Запустите PowerShell от имени пользователя, которому разрешён доступ к SQL Server.
4. Выполните .\EKK_Deploy.ps1. Скрипт копирует приложение в C:\EKK, запрашивает строку подключения и пароль RabbitMQ скрытым вводом и запускает Worker в текущей консоли.
5. Worker работает в интерактивном режиме; остановка — Ctrl+C. Windows-служба не создаётся.

Для локальной сборки из корня репозитория:

    dotnet restore .\EKK.sln
    dotnet build .\EKK.sln --configuration Release
    dotnet publish .\src\Ekk.Host\Ekk.Host.csproj -c Release -r win-x64 --self-contained true
