# ЕКК — единое корпоративное ядро

Новая реализация ЕКК на .NET 10 LTS.

## Текущий объём

Первая реализация — обработка операции `CompleteEntityRegistration`:

- только система-владелец может завершить регистрацию Entity;
- допустимые переходы: `RegistrationPending → Registered` и `RegistrationPending → RegistrationError`;
- текущий статус, история регистрации и результат идемпотентности фиксируются одной SQL-транзакцией;
- повтор с тем же ключом и тем же содержимым возвращает сохранённый ответ;
- повторное использование ключа с другим содержимым возвращает `IDEMPOTENCY_KEY_REUSE`;
- повторное завершение тем же результатом не создаёт дополнительную запись истории;
- EntityId не удаляется и не переиспользуется.

## Структура

```
EKK.sln
src/
  Ekk.Host/
    Program.cs
    RabbitMqRequestWorker.cs
    RegistrationCompletionService.cs
    Ekk.Host.csproj
    appsettings.json
```

## Требования

- .NET 10 SDK
- SQL Server с уже развёрнутой согласованной схемой ЕКК
- RabbitMQ с очередью `ekk.requests` и exchange `ekk.responses`

## Настройка

Не храните пароли в репозитории. Перед запуском задайте переменные окружения:

- `Sql__ConnectionString`
- `RabbitMq__UserName`
- `RabbitMq__Password`
- `Ekk__SourceSystemInstanceKey`
- `Ekk__SystemCode`
- `Ekk__InstanceCode`

`SourceSystemInstanceKey` задаётся в конфигурации экземпляра приложения и не принимается из тела сообщения. RabbitMQ-учётная запись должна быть выделена для этого экземпляра и иметь минимальные права.

## Сборка и запуск

Из корня репозитория:

```powershell
dotnet restore .\EKK.sln
dotnet build .\EKK.sln --configuration Release
dotnet run --project .\src\Ekk.Host\Ekk.Host.csproj
```

## Формат запроса

Сообщение JSON с общим конвертом ЕКК:

```json
{
  "MessageId": "01900000-0000-7000-8000-000000000001",
  "CorrelationId": "01900000-0000-7000-8000-000000000002",
  "MessageType": "CompleteEntityRegistration",
  "SchemaVersion": 1,
  "CreatedAt": "2026-11-04T12:00:00Z",
  "Payload": {
    "EntityId": "01900000-0000-7000-8000-000000000003",
    "Result": "Registered",
    "IdempotencyKey": "01900000-0000-7000-8000-000000000004",
    "ErrorCode": null,
    "ErrorMessage": null
  }
}
```

Для результата `RegistrationError` поле `ErrorCode` обязательно, `ErrorMessage` необязательно.

## Важные ограничения текущего этапа

- SQL-схема не изменяется.
- HTTP/IIS, XML-обмен и COM не используются.
- Транспорт пока обслуживает только `CompleteEntityRegistration`; остальные согласованные операции добавляются отдельными этапами.
- До подключения к рабочей БД необходимо проверить соответствие строки подключения и RabbitMQ credentials конкретному экземпляру SourceSystemInstance.
- Текущая реализация предполагает, что владелец Entity определяется через `Entity.CreatedBySourceSystemInstanceKey`.
