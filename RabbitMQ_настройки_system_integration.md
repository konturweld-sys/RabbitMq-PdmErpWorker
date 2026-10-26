# RabbitMQ — пользователь общей интеграции

Пользователь: `system_integration`  
Virtual host: `/`  
Exchange: `1c.pdm.export` (direct, durable)

Queues:
- `erp.kinds_of_nomenclature` (durable)
- `erp.nomenclature` (durable)
- рекомендуется DLX `1c.integration.dlx` и DLQ `erp.integration.dead`

Bindings:
- `erp.kinds_of_nomenclature` ← `1c.pdm.export`, routing key `pdm.kind.item`
- `erp.nomenclature` ← `1c.pdm.export`, routing key `pdm.nomenclature.item`

Выполнить на RabbitMQ host от администратора (PowerShell с установленным `rabbitmqctl` в PATH или CMD):

```powershell
rabbitmqctl add_user system_integration "REPLACE_WITH_LONG_RANDOM_SECRET"
rabbitmqctl add_vhost /
rabbitmqctl set_permissions -p / system_integration ".*" ".*" ".*"
```

ВНИМАНИЕ: эта базовая команда выдаёт права configure/write/read на все объекты виртуального хоста `/`. Это соответствует общему пользователю для нескольких интеграций, но является широкими правами. Предпочтительно отдельный vhost на группу интеграций или права по регулярным выражениям на согласованный список exchange/queues. Не передавайте пароль в чат и не храните его в исходном коде.

Для HTTP Management API нужны также права на Management UI/API. AMQP permissions и Management tags — разные вещи. Если `system_integration` будет публиковать/читать через HTTP API, назначьте тег `management` только при необходимости, предпочтительнее ограниченный reverse proxy/отдельная политика. Для AMQP Worker достаточно AMQP permissions; management tag ему не нужен.

Пример создания топологии через `rabbitmqadmin` (если команда доступна на сервере):
```powershell
rabbitmqadmin -V / declare exchange name=1c.pdm.export type=direct durable=true
rabbitmqadmin -V / declare queue name=erp.kinds_of_nomenclature durable=true
rabbitmqadmin -V / declare queue name=erp.nomenclature durable=true
rabbitmqadmin -V / declare binding source=1c.pdm.export destination_type=queue destination=erp.kinds_of_nomenclature routing_key=pdm.kind.item
rabbitmqadmin -V / declare binding source=1c.pdm.export destination_type=queue destination=erp.nomenclature routing_key=pdm.nomenclature.item
```

В production использовать TLS (AMQPS), секрет из защищённого хранилища и durable queues + persistent messages + publisher confirms + consumer manual ACK. Management API `/api/queues/.../get` с `ack_requeue_false` не использовать для обработки, так как сообщение подтверждается до завершения транзакции ERP.