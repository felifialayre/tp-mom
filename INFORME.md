# Decisiones de diseño

## Clase base compartida

Siendo que `Queue` y `Exchange` comparten gran parte de la lógica en momentos como el cierre de conexión (`close()`), el comienzo de consumisión (`start_consuming()`) se decidió de implementar una clase base `_RabbitMQBase` para no repetir código entre los distintos modelos de comunicación.

## Configuración diferida al `start_consuming`

El `basic_qos` (prefetch) y el `queue_bind` de las routing keys se aplican recién en
`start_consuming`, no en el `__init__`. Un mismo objeto puede usarse solo para
**producir**, y en ese caso esas configuraciones no tienen sentido:

- **`prefetch_count`**: solo afecta al consumo. En la work queue se usa `prefetch_count=1`
  para lograr fairness en el dispatch entre consumidores que compiten por la misma cola. En el
  exchange no se llama a `basic_qos`: cada consumidor tiene su propia cola (no compiten),
  así que el default (prefetch ilimitado) permite al usuario ajustarlo a su gusto.
- **`queue_bind`**: bindear a las routing keys solo importa para recibir mensajes. Un
  productor publica con la routing key, no necesita ninguna cola bindeada.

## Colas: durable vs. exclusive

- **Work queue**: se declara `durable=True`. Es un recurso compartido y con nombre fijo
  que debe persistir independientemente de las conexiones (varios productores/consumidores podrían potencialmente usarla). Los mensajes enviados también serán persistentes haciendo así que si no hay workers consumiendo las _tareas_ al reconectarse puedan seguir trabajando.
- **Cola del exchange (consumidor)**: se declara anónima (`queue=''`) y `exclusive=True`.
  Es privada de cada consumidor y RabbitMQ la elimina automáticamente al cerrarse la
  conexión que la creó, evitando dejar colas huérfanas en el broker.

## `close()` no idempotente

`close()` cierra únicamente la conexión (cerrar la conexión cierra en cascada sus canales,
así que cerrar el canal aparte sería redundante y podría dejar la conexión abierta si
fallara). Se decide **no** hacer `close()` idempotente: cerrar una conexión ya cerrada
lanza `MessageMiddlewareCloseError`. El enunciado no exige idempotencia para `close`
(a diferencia de `stop_consuming`, que sí debe ser un no-op si no se estaba consumiendo),
por lo que un doble cierre se considera un error explícito del uso, no algo a silenciar.

## Manejo de errores y ownership de recursos

- Las excepciones de `pika` se traducen a las del middleware: `AMQPConnectionError` →
  `MessageMiddlewareDisconnectedError`; el resto de `AMQPError` →
  `MessageMiddlewareMessageError`. Se usa `raise ... from e` para conservar la causa original.
- La conexión se cierra ante un error **solo en el `__init__`**: si la construcción falla,
  el usuario nunca recibe el objeto y no tiene forma de invocar `close()`, por lo que la
  limpieza es responsabilidad del constructor. En el resto de los métodos el objeto ya
  existe y el usuario es dueño del recurso: ante un error se propaga la excepción y es él
  quien decide cerrar o reintentar.
