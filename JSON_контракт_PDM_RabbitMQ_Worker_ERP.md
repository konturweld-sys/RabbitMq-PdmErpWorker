# Контракт PDM → RabbitMQ → Worker → ERP

Версия контракта: 1.0  
Кодировка: UTF-8. Имена полей JSON — camelCase. `messageId` — UUID, генерируется один раз на событие и сохраняется при повторной доставке.

## 1. Сообщение о виде номенклатуры

Routing key: `pdm.kind.item`  
Queue: `erp.kinds_of_nomenclature`

```json
{
  "schemaVersion": 1,
  "messageId": "2c5fbb7e-2f6d-4b4f-8b1c-7f6f1a2b3c4d",
  "eventType": "nomenclature-kind.upsert",
  "sourceSystem": "1C_PDM",
  "sourceRef": "UUID-источника-в-PDM",
  "name": "Крепёж",
  "itemType": "Товар",
  "usesCharacteristics": false,
  "unit": "шт"
}
```

`sourceRef` — UUID ссылки вида в PDM. `unit` — строковый код/имя единицы по согласованному справочнику; Worker не должен угадывать соответствие единиц. ERP должен подтвердить, что единица сопоставлена с существующей `УпаковкиЕдиницыИзмерения`.

## 2. Сообщение о позиции номенклатуры

Routing key: `pdm.nomenclature.item`  
Queue: `erp.nomenclature`

```json
{
  "schemaVersion": 1,
  "messageId": "d2b5b5e1-0eaa-4c1c-9b5a-0c2e0c3f4d5e",
  "eventType": "nomenclature.created",
  "sourceSystem": "1C_PDM",
  "sourceRef": "UUID-элемента-номенклатуры-в-PDM",
  "itemName": "Болт M10-LH-6gx60.58.10.013(S16) ГОСТ 7798-70",
  "kindSourceRef": "UUID-вида-номенклатуры-в-PDM",
  "kindName": "Крепёж",
  "itemType": "Товар",
  "unit": "шт",
  "usesCharacteristics": false
}
```

Обязательные правила:
- позиция создаётся только после того, как `kindSourceRef` сопоставлен существующему виду ERP;
- `usesCharacteristics` всегда `false`;
- отдельные варианты изделия — отдельные элементы справочника `Номенклатура`, не характеристики;
- точное имя сохраняется как `itemName`;
- уникальность события определяется `messageId`; идемпотентность предметной записи — сочетанием `sourceSystem + sourceRef`;
- не использовать только поиск по похожему имени как глобальный идентификатор источника.

## 3. Запрос Worker → HTTP-сервис ERP

POST к опубликованному сервису ERP `СоздатьИзделие`, шаблон `/items`:

```json
{
  "schemaVersion": 1,
  "messageId": "d2b5b5e1-0eaa-4c1c-9b5a-0c2e0c3f4d5e",
  "eventType": "nomenclature.created",
  "sourceSystem": "1C_PDM",
  "sourceRef": "UUID-элемента-номенклатуры-в-PDM",
  "itemName": "Болт M10-LH-6gx60.58.10.013(S16) ГОСТ 7798-70",
  "kindSourceRef": "UUID-вида-номенклатуры-в-PDM",
  "kindName": "Крепёж",
  "itemType": "Товар",
  "unit": "шт",
  "usesCharacteristics": false
}
```

## 4. Ответ ERP

Успех (создано или повтор уже обработанного сообщения):
```json
{
  "success": true,
  "code": "CREATED",
  "messageId": "d2b5b5e1-0eaa-4c1c-9b5a-0c2e0c3f4d5e",
  "sourceRef": "UUID-элемента-номенклатуры-в-PDM",
  "nomenclatureRef": "UUID-элемента-номенклатуры-в-ERP",
  "duplicateMessage": false
}
```

Вид ещё не синхронизирован (повторяемая ошибка):
```json
{
  "success": false,
  "code": "KIND_NOT_FOUND",
  "messageId": "d2b5b5e1-0eaa-4c1c-9b5a-0c2e0c3f4d5e",
  "retryable": true,
  "message": "Вид номенклатуры ещё не синхронизирован."
}
```

После HTTP-ответа Worker:
- HTTP 2xx и `success=true`: ACK сообщения RabbitMQ;
- `retryable=true`: повтор с задержкой либо отложенная очередь, не бесконечный немедленный requeue;
- постоянная ошибка контракта: отправить в DLQ и ACK исходного сообщения;
- timeout/обрыв без подтверждённого ответа: повторить с тем же `messageId`.

## 5. Поля, которые текущий ERP-приёмник ещё не реализует

Текущий модуль из `ERP_cfe_24102026_testfix7.zip` читает `itemName`, `itemType`, `unit`, `usesCharacteristics`, `sourceRef`, `messageId`, но пока не читает/не использует `kindSourceRef` и `kindName`; он создаёт элемент без установки обязательного `ВидНоменклатуры`. Перед production нужно заменить обработчик ERP и расширить идемпотентность по `sourceSystem + sourceRef`, а не только по `messageId`.

Текущий регистр `Расш1_ОбработанныеСообщения` имеет измерение `MessageId` и ресурс `NomenclatureRef`; он не обеспечивает отдельного постоянного сопоставления `sourceRef` вида PDM → ссылка вида ERP. Для строгого сопоставления видов нужен отдельный регистр соответствий (источник + UUID вида → ссылка ERP), либо уже существующий в ERP реестр, если он есть.