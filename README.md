# ЕКК — единое корпоративное ядро

Worker ЕКК на .NET 10: RPC через RabbitMQ, транзакционная регистрация в SQL Server и публикация событий через transactional Outbox.

## Операции

- **RegisterEntity** — создаёт Entity с UUIDv7 только если активный EntityType назначен системе-владельцу настроенного SourceSystemInstance. Начальный статус — RegistrationPending.
- **RegisterSourceObject** — связывает локальный объект с Entity по уникальной идентичности (SourceSystemInstanceKey, ExternalObjectType, ExternalObjectId).
- **CompleteEntityRegistration** — переводит Entity из RegistrationPending в Registered или RegistrationError и одновременно записывает историю.

Все операции используют RequestIdempotency. Повтор с тем же ключом и тем же содержимым возвращает сохранённый ответ; повторное использование ключа с другим содержимым отклоняется.

## Транзакции и доставка

При регистрации Entity или SourceObject в одной SQL-транзакции выполняется бизнес-изменение, увеличивается Entity.LastEventSequence, создаётся ekk.OutboxMessage, сохраняется ответ идемпотентности и выполняется COMMIT.

Outbox Publisher использует persistent delivery и RabbitMQ Publisher Confirms. Статус Published устанавливается только после подтверждения брокера. При ошибке увеличивается AttemptCount, задаётся NextAttemptAt с ограниченной экспоненциальной задержкой и сохраняется LastError. Сбой после подтверждения RabbitMQ, но до обновления SQL может вызвать повторную публикацию; потребитель должен дедуплицировать по MessageId (at-least-once).

Входящий запрос ACK-ается только после обработки и подтверждённой публикации ответа. Ошибка SQL или RabbitMQ приводит к NACK/requeue. Обработка последовательная, без конкурентного доступа к RabbitMQ IModel.

## RabbitMQ

- Request queue: ekk.requests.
- RPC response exchange: ekk.responses.
- Outbox event exchange: ekk.events.
- RPC routing key: response.{SystemCode}.{InstanceCode}.{MessageType}.
- Event routing keys: entity.created и entity.source-object-registered.
- Конверт RPC: MessageId, CorrelationId, MessageType, SchemaVersion=1, CreatedAt, Payload.

Очередь и exchange должны быть заранее созданы; Worker не переобъявляет топологию.

## Соответствие фактической схеме БД

- Worker не выполняет DDL, миграции или заполнение справочников.
- В фактической ekk.SourceObject нет столбца Namespace; применяется существующий уникальный ключ без Namespace. Добавление Namespace требует отдельной согласованной миграции.
- `EntityTypeOwner` связывает каждый EntityType с одной системой-владельцем (PK по EntityTypeKey). Экземпляр может выполнить RegisterEntity только для типов своей системы.
- При запуске Worker проверяет соответствие `Ekk:SourceSystemInstanceKey`, `Ekk:SystemCode` и `Ekk:InstanceCode` активным записям `SourceSystem` и `SourceSystemInstance`.
- `RegisterSourceObject` по-прежнему разрешён только экземпляру, который создал Entity.
- Скрипт `sql/EntityTypeOwnership_And_TestInstances.sql` создаёт `EntityTypeOwner` и идемпотентно добавляет системы PDM/ERP/MES и тестовые экземпляры PDM_TEST/ERP_TEST. Он не заполняет EntityType и назначения владельцев: для этого необходим утверждённый перечень кодов EntityType.
- Перед запуском требуется проверить ограничения БД и RabbitMQ topology.
- Outbox Publisher рассчитан на один экземпляр на одну БД; для нескольких экземпляров нужен согласованный механизм захвата строк Outbox.

## Конфигурация

Секреты не хранятся в репозитории. Задайте переменные окружения:
- Sql__ConnectionString
- RabbitMq__UserName
- RabbitMq__Password
- Ekk__SourceSystemInstanceKey
- Ekk__SystemCode
- Ekk__InstanceCode

SourceSystemInstanceKey задаётся конфигурацией установленного экземпляра и не принимается из сообщения.

## Сборка и запуск

Из PowerShell в корне репозитория:
    dotnet restore .\EKK.sln
    dotnet build .\EKK.sln --configuration Release
    dotnet run --project .\src\Ekk.Host\Ekk.Host.csproj
