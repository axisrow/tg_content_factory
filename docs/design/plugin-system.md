# Plugin system: дизайн-контракт

**Статус:** proposal, подзадача 325.0

**Дата:** 2026-08-27
**Область:** контракт плагина, доступ к ядру, безопасность и совместимость

Этот документ фиксирует архитектурное решение для будущей plugin-системы TG
Agent. Он **не реализует** registry, discovery, установку из Git, lifecycle
wiring или reference-плагин. Эти работы относятся к 325.1–325.4 и начинаются
только после решения владельца по настоящему документу.

## 1. Контекст и цели

Плагин — это отдельно поставляемый Python-пакет, который расширяет запущенное
ядро TG Agent через явно объявленный контракт. Плагин не должен зависеть от
внутренней структуры `src/` и не должен модифицировать существующие модули
через monkey-patching.

Цели:

1. Иметь небольшой versioned host-контракт с предсказуемым lifecycle.
2. Дать плагину контролируемый доступ к данным, конфигурации и Telegram, не
   выдавая ему секреты и внутренние объекты без необходимости.
3. Различать совместимость плагина с **API хоста** и версию самого приложения.
4. Сделать происхождение и обновление внешнего или приватного кода явным,
   проверяемым и обратимым.
5. Не создавать иллюзию sandbox для Python-кода, который выполняется в том же
   процессе, что и ядро.

Не является целью этого этапа:

- реализация интерфейса, registry и discovery;
- установка, обновление или удаление пакетов;
- определение конкретных web/CLI hooks;
- запуск недоверенного кода в безопасной песочнице;
- совместимость со всеми будущими версиями ядра без проверки.

## 2. Принятые архитектурные решения

| Область | Решение |
|---|---|
| Форма поставки | Python distribution; обнаружение в дальнейшем выполняется через объявленную entry point group, а не сканированием модулей или импортом произвольных файлов. |
| Исполнение | В первой версии — in-process. Это означает, что разрешённый плагин считается доверенным кодом с правами процесса. |
| Контракт | Один экземпляр плагина на процесс, неизменяемые metadata, асинхронные `initialize` и `shutdown`. |
| Доступ к ядру | Через versioned `PluginContext` и capability-limited ports; прямой доступ к внутренним полям ядра запрещён контрактом. |
| Telegram | Сырые Telethon-клиенты, session strings и внутренние lease-объекты не являются API плагина. |
| Установка | Только явное действие оператора, с allowlist, фиксацией исходной ревизии и возможностью rollback; автоматический install/update не допускается. |
| Совместимость | Проверяется до активации по отдельному `core_plugin_api` и версиям портов, а не только по версии пакета `tg-agent`. |
| Ошибка | Необязательный плагин не должен ломать запуск ядра; несовместимый или не прошедший init плагин пропускается и попадает в диагностику. |

## 3. Контракт плагина

### 3.1. Идентичность и metadata

Каждая реализация обязана предоставить неизменяемое описание до начала работы.
Минимальный набор полей:

| Поле | Правило |
|---|---|
| `id` | Стабильный уникальный идентификатор в lowercase (`vendor.feature` или `vendor-feature`). Он не меняется при переименовании display name и используется для namespace данных и настроек. |
| `name` | Человекочитаемое название для UI и логов; не используется как ключ. |
| `version` | Версия выпуска плагина по SemVer (`MAJOR.MINOR.PATCH`). |
| `core_plugin_api` | Диапазон SemVer API хоста, например `>=1.0,<2.0`. Это не версия релиза TG Agent. |
| `ports` | Диапазоны версий требуемых портов (`database`, `config`, `client_pool`). Отсутствующий порт означает, что плагин им не пользуется. |
| `capabilities` | Явный список запрашиваемых прав. По умолчанию список пуст. |
| `description` | Краткое назначение для списка установленных плагинов. Не является основанием для выдачи прав. |
| `publisher` | Идентичность издателя/владельца, используемая политикой доверия и аудитом; один только текст этого поля не доказывает подлинность. |

`id` нельзя переиспользовать для другого продукта: удалённый плагин и новый
плагин с тем же `id` считаются одной линией данных. Изменение владельца или
семантики требует нового идентификатора либо отдельного решения о миграции.

Metadata должны быть доступны для проверки совместимости и политики до
активации плагина. Имя пакета, имя entry point и `id` — разные сущности и не
должны неявно подменять друг друга.

### 3.2. Интерфейс и lifecycle

Контракт состоит из следующих операций:

| Операция | Семантика |
|---|---|
| `metadata` | Возвращает immutable metadata из раздела 3.1. Доступ к БД, сети и Telegram на этой стадии запрещён. |
| `initialize(context)` | Асинхронно получает `PluginContext`, проверяет собственную конфигурацию и регистрирует расширения через предоставленные registration handles. Вызывается не более одного раза для экземпляра. |
| `health()` | Асинхронно возвращает plugin-specific health report после успешного `initialize`: общий статус и именованные проверки зависимостей, без которых заявленные hooks неработоспособны. Операция read-only, ограничена timeout и не выполняет миграции, restart или self-healing. |
| `shutdown()` | Асинхронно останавливает работу и освобождает только ресурсы плагина. Должен быть безопасен при частично завершённом `initialize`; повторный вызов не должен приводить к ошибке. |

Дополнительные требования:

- lifecycle вызывается только event loop-ом хоста; блокирующие операции плагин
  выносит в собственный executor по правилам ядра;
- `initialize` не должен создавать неконтролируемые фоновые задачи. Для них
  используется host-managed task handle, чтобы ядро могло отменить их при
  остановке;
- `health` вызывается только для lifecycle-state `active`; timeout, исключение,
  `degraded` или `unhealthy` не считаются успешной проверкой обновления;
- plugin-код не импортирует `src.*` как часть публичного контракта, кроме
  опубликованных versioned API-модулей;
- плагин должен корректно работать при повторном запуске процесса после
  незавершённого shutdown;
- все hooks получают timeout, cancellation и структурированное логирование;
- плагин не может регистрировать второй экземпляр с тем же `id` в одном
  runtime.

В первой версии lifecycle не обещает hot reload. Изменение версии, исходной
ревизии или разрешений применяется после контролируемого restart процесса.

### 3.3. Registration и зависимости

В будущих задачах плагин будет регистрировать расширения только через
ограниченные handles, выданные `PluginContext`: например, handler, scheduled
job или route. Конкретный перечень hooks не входит в 325.0. Нельзя считать
публичным API импорт или вызов существующего сервиса напрямую.

Если плагину нужны другие плагины, зависимость объявляется metadata и
проверяется до lifecycle. Нельзя решать зависимости побочным эффектом порядка
импортов. Цикл зависимостей и отсутствие обязательной зависимости делают
плагин неактивным с диагностикой причины.

## 4. Доступ к ядру через `PluginContext`

Сейчас `AppContainer` собирает `Database`, `AppConfig` и live `ClientPool` в
одном runtime, тогда как web-runtime может использовать snapshot-заменители.
Плагин получает не весь контейнер, а отдельный контекст следующего вида:

| Порт контекста | Разрешённое содержимое | Явно не входит в контракт |
|---|---|---|
| `database` | Versioned database port с доменными read/write-операциями и namespaced storage плагина. | `aiosqlite.Connection`, путь к файлу, `_db`, `_write_lock`, произвольный SQL и репозитории, не перечисленные выданным портом. |
| `config` | Неизменяемый validated view общих безопасных настроек и секции конкретного плагина. | `os.environ`, исходный YAML, секреты провайдеров, session encryption key и mutating methods. |
| `client_pool` | Versioned Telegram port с операциями, разрешёнными capability и режимом runtime. | `ClientPool` целиком, Telethon client/session, StringSession, внутренние leases, reconnect/disconnect и обход flood/rate limits. |
| `logger` | Logger с автоматически добавленными `plugin_id`, версией и runtime mode. | Запись секретов и несанитизированных session/token значений. |
| `tasks` | Host-managed task handles с cancellation и lifecycle ownership. | Самостоятельное владение loop или бесконтрольные daemon threads. |

Названия `Database`, `AppConfig` и `ClientPool` здесь обозначают точки
интеграции с существующим ядром. На границе плагина они должны быть
адаптированы к versioned ports. Передача текущих concrete-объектов напрямую
сделала бы публичными их внутренние методы (`Database.execute_write`, поля
состояния `ClientPool`) и связала бы плагин с внутренними рефакторами.

### 4.1. Database

Рекомендуемая модель — два уровня доступа:

1. read-only доменные операции над опубликованными моделями ядра;
2. отдельное хранилище плагина, изолированное namespace-ом `plugin_id` и
   обслуживаемое миграциями, которыми владеет сам плагин через host API.

Write capability выдается только при явном запросе и политике владельца.
Транзакции, атомарность и retry остаются ответственностью database port; плагин
не получает соединение и не управляет commit/rollback самостоятельно.

Плагин не должен писать в таблицы ядра напрямую. Если будущий hook требует
изменения доменного состояния, это делается отдельным versioned core operation,
а не SQL-обходом. Удаление/миграция данных плагина должны быть видны в audit и
поддерживать rollback на уровне схемы, где это возможно.

Миграции plugin storage являются host-managed lifecycle phase, а не побочным
эффектом `initialize`. Пакет декларативно указывает исходную и целевую версии
схемы, forward-план и требования к восстановлению; хост проверяет этот план и
выполняет его до передачи storage port новой ревизии. Плагин не может самовольно
менять схему при импорте или init.

Перед первой storage-миграцией update хост обязан создать согласованную точку
восстановления plugin namespace, включающую схему, данные и storage revision.
Предпочтительный механизм — shadow storage revision: snapshot текущего
namespace мигрируется отдельно, а принятая ревизия остаётся неизменной до
атомарного переключения указателя. Если backend не поддерживает shadow copy,
допустим только cross-runtime fence из раздела 7.2.1, который останавливает все
plugin writes, и транзакционный snapshot/restore. Process-local lock или
остановка одной instance недостаточны. Update с destructive migration нельзя
начинать, если хост не может доказать атомарное восстановление предыдущего
состояния.

### 4.2. Config

Конфигурация плагина размещается в выделенном namespace, ключом которого служит
`plugin_id`. Ядро загружает и валидирует её до `initialize`; неизвестные или
невалидные поля не должны молча превращаться в права.

Плагин получает snapshot, поэтому изменение `config.yaml` или окружения не
меняет его настройки посреди операции. Для применения новой конфигурации нужен
контролируемый reload/restart, а поддержка hot reload будет отдельным решением.

Секретные значения не передаются через общий `config` view. Если отдельный
плагин действительно требует секрет, это должен быть отдельный явно выданный
secret capability с redacted logging и без обратного чтения всего окружения.

### 4.3. ClientPool

Доступ к Telegram всегда capability-based и lease-safe:

- `telegram.read` разрешает только предусмотренные чтения с host-managed
  timeout, retry и flood-wait policy;
- `telegram.publish`/`telegram.send` — отдельное повышенное разрешение, не
  включаемое вместе с read по умолчанию;
- plugin не выбирает произвольный аккаунт и не меняет preferred-phone,
  reconnect или состояние пула;
- irreversible send operation должна проходить общий audit/confirmation
  механизм ядра и не может быть заменена прямым вызовом Telethon;
- при отсутствии live pool (например, web snapshot-runtime) capability
  объявляется unavailable, а не маскируется пустым mock-объектом.

Это сохраняет единую политику rate limit, lease и shutdown для ядра и
плагинов. Конкретные методы port и правила подтверждения публикуются вместе с
соответствующей версией API, а не копируются в каждый плагин.

## 5. Модель безопасности

### 5.1. Граница доверия

Python-плагин, импортированный в процесс TG Agent, технически может читать
память процесса, импортировать любые доступные модули, открывать сеть и
пытаться читать окружение. Capability-проверки защищают от случайного и
обычного ошибочного использования API, но **не являются sandbox от вредоносного
Python-кода**.

Следствие: внешний и приватный репозитории отличаются происхождением и
операционной политикой, но не уровнем привилегий после загрузки. Private не
означает trusted автоматически, а public не означает malicious автоматически.
Владелец отвечает за доверие к исходному коду и цепочке поставки.

Рекомендуемые уровни доверия:

| Уровень | Источник | Разрешённый режим |
|---|---|---|
| A | First-party код, прошедший review и поставляемый вместе с продуктом | In-process; capability задаётся владельцем. |
| B | Одобренный private repository конкретной организации | In-process только после review, pin ревизии и явного allowlist. |
| C | Одобренный внешний/public repository | In-process только после review, pin ревизии и отдельного одобрения издателя. |
| D | Непроверенный пакет, URL или локальная папка | Не загружать. Для такого кода нужен будущий out-of-process protocol/sandbox, которого этот дизайн не реализует. |

### 5.2. Политика внешних и приватных репозиториев

Эта политика является обязательными требованиями к будущей установке (325.3):

1. **Явное одобрение.** Никаких install/update из runtime, из metadata плагина
   или из непроверенной команды. Оператор утверждает `id`, издателя,
   repository, разрешённые capabilities и версию.
2. **Allowlist источников.** Разрешаются только заранее заданные hosts,
   owners и repositories. Redirect на другой host, submodule с неизвестным
   источником и произвольный dependency source требуют отдельного решения.
3. **Неизменяемая ревизия.** Запись установки фиксирует commit SHA (или
   artifact digest), а не только ветку или тег. Тег может быть передвинут.
4. **Проверка происхождения.** Для поддерживаемого способа поставки проверяются
   подпись/attestation издателя и digest собранного артефакта. Если проверка
   невозможна, установка останавливается или требует явного override с audit.
5. **Изоляция установки.** Fetch и build выполняются в staging-окружении до
   импорта в рабочий процесс. Проверяются metadata, зависимости, заявленные
   capabilities и диапазоны совместимости; активная установка не изменяется
   частично.
6. **Запрет автоподхвата зависимостей.** Lock фиксирует транзитивные
   зависимости и их источники/digests. Импорт нового пакета не должен тихо
   менять весь runtime.
7. **Секреты Git.** Credentials для private repository берутся из внешнего
   credential helper/SSH agent или secret store. Token/password нельзя хранить
   в URL, lock-файле, `config.yaml` или логах; deploy key должен быть
   read-only и ограничен нужным repository.
8. **Обновление и rollback.** Обновление staged, проверяется и применяется
   после restart. Предыдущая рабочая ревизия сохраняется до успешного
   plugin-specific acceptance gate из раздела 7.1; общий health приложения не
   заменяет эту проверку. Rollback является единым восстановлением code
   revision, lock metadata и plugin storage state по правилам раздела 7.2. Hot
   replacement не используется.

Подписание пакета не отменяет review: скомпрометированный ключ может подписать
вредоносный код. И наоборот, commit SHA без проверки владельца не доказывает,
что исходный код разрешён к запуску.

### 5.3. Runtime-защита и аудит

- Перед `initialize` ядро проверяет заявленные capabilities против политики
  установки. Незапрошенная capability не выдаётся даже если плагин пытается
  обратиться к соответствующему полю.
- Логи содержат `plugin_id`, plugin version, source revision, outcome init и
  причины отказа. Секреты, session strings, API keys и приватные Git URLs с
  credentials редактируются.
- Установка, активация, отключение, ошибка init, send/publish и миграции
  хранилища — audit events с оператором и временем.
- Ошибка необязательного плагина изолируется: плагин переводится в disabled,
  registration handles отзываются, host-managed tasks отменяются. Ошибка не
  должна отменять остановку остальных компонентов.
- Сбой или зависание `shutdown` ограничивается timeout; ядро продолжает
  закрывать следующие компоненты и фиксирует инцидент.

Для настоящей защиты от вредоносного плагина потребуется отдельный процесс,
минимальный RPC-контракт, отдельный OS-user и политика network/filesystem. Это
намеренно не входит в текущую in-process версию.

## 6. Версионирование и совместимость с ядром

Нужно различать три независимые версии:

1. **Plugin release version** — SemVer конкретного плагина (`version`).
2. **Core plugin API version** — SemVer lifecycle/context/registration-контракта
   хоста (`core_plugin_api`).
3. **Core application release** — текущая версия `tg-agent` (сейчас в metadata
   проекта указана `0.2.3`). Она описывает релиз приложения, но не заменяет
   стабильный plugin API.

Для точных границ контекста дополнительно versioned независимые порты:
`database`, `config`, `client_pool`. Плагин объявляет минимальную и
максимальную совместимую версию каждого используемого порта. Проверяются и
агрегированный host API, и каждый запрошенный port.

Правила SemVer:

| Изменение host API | Версия | Действие ядра |
|---|---:|---|
| Исправление ошибки без изменения обещанной семантики | PATCH | Совместимо. |
| Новое optional поле/operation, сохранение старого поведения | MINOR | Старый плагин продолжает работать; новый плагин может запросить новую minor-версию. |
| Удаление, переименование, изменение типов/семантики, ослабление или изменение security boundary | MAJOR | Плагин со старым major не загружается в новый major. |

Релиз ядра с `core_plugin_api 1.4` может загружать плагин, требующий
`>=1.2,<2.0`, если все его ports совместимы. Релиз ядра `2.0` не должен
загружать такой плагин молча и не должен подменять отсутствующие permissions
или операции no-op-ом. Несовместимость показывается оператору до запуска.

Дополнительные правила:

- compatibility ranges валидируются до регистрации hooks;
- capability и port API не понижаются автоматически: если плагин требует
  `client_pool >=1.1`, host с `1.0` отказывает в активации;
- deprecated API получает предупреждение и срок удаления, но остаётся
  функциональным весь заявленный compatibility window;
- изменение схемы данных плагина версионируется отдельно от host API и должно
  иметь upgrade/rollback plan;
- до публикации `core_plugin_api 1.0` любые изменения считаются потенциально
  breaking и требуют пересмотра документа.

## 7. Порядок запуска и остановки

Рекомендуемый порядок для будущего integration work:

1. Ядро загружает и валидирует конфигурацию.
2. Инициализирует Database и миграции ядра.
3. Создаёт restricted ports и контекст runtime mode.
4. В worker-runtime инициализирует live ClientPool; в web snapshot-runtime
   Telegram live capability остаётся unavailable.
5. Проверяет metadata, policy, host/port compatibility и зависимости.
6. Инициализирует плагины в детерминированном dependency order.
7. Только после успешной регистрации запускает consumers, dispatchers,
   agent-интеграции и scheduler, которые могут увидеть hooks плагинов.

Остановка идёт в обратном направлении: сначала останавливаются producers и
consumers, затем plugin tasks и `shutdown`, затем Telegram pool и только потом
Database. Один сломавшийся плагин не должен помешать закрытию секретов,
соединений и остальных плагинов.

Политика по умолчанию — plugin optional: incompatibility/init failure не
останавливает ядро. Для плагина, без которого deployment не имеет смысла,
нужен отдельный `required` флаг в утверждённой политике; такой failure должен
делать startup fail-closed, а не запускать частично работающую систему.

### 7.1. Plugin health и принятие обновления

Успешный startup ядра или общий `/health` не означает, что обновлённый плагин
работает: optional-плагин может остаться disabled после ошибки `initialize`, а
приложение продолжит обслуживать запросы. Поэтому каждое обновление получает
отдельный host-owned acceptance record. До активации в нём фиксируются:

- ожидаемые `plugin_id`, plugin version, source revision и artifact digest;
- runtime targets, в которых плагин должен быть активен;
- утверждённые capabilities и набор обязательных hook IDs;
- исходная и целевая plugin storage revision и идентификатор restore point;
- shared update epoch и множество runtime leases, участвующих в barrier;
- предыдущая принятая ревизия, доступная для rollback.

После restart обновление принимается только если для каждого ожидаемого
runtime target одновременно выполнены все условия:

1. Хост самостоятельно подтверждает, что загруженная distribution соответствует
   ожидаемым `plugin_id`, version, source revision и digest. Эти значения нельзя
   брать только из self-report плагина.
2. Registry фиксирует lifecycle-state `active` именно для ожидаемой ревизии;
   состояния `discovered`, `disabled`, `failed` и `stopping` неприемлемы.
3. Все обязательные hooks зарегистрированы этой instance/revision, включены и
   остались привязаны к выданным registration handles. Заявления самого плагина
   о регистрации hooks недостаточно.
4. Хост подтверждает ожидаемую storage revision, успешное завершение
   host-managed migration и доступность проверенной точки восстановления.
5. `health()` завершился в пределах timeout со статусом `healthy`, а все
   обязательные именованные проверки успешны. Пустой report, exception,
   cancellation, `degraded` и timeout означают failure.
6. Проверка успешна сразу после `initialize` и повторно после заданного policy
   stabilization window; это защищает от плагина, который регистрируется, но
   сразу теряет фоновые задачи или внешнюю зависимость.

Acceptance record сохраняется атомарно только после выполнения всего gate.
Лишь после этого предыдущая ревизия может стать кандидатом на очистку. При
любом failure или истечении acceptance timeout update остаётся непринятым,
старая ревизия не удаляется, а следующий контролируемый restart выполняет
rollback. Статус optional влияет только на возможность продолжить startup
ядра: он **не** разрешает принять неактивное или нездоровое обновление плагина.

### 7.2. Rollback кода и plugin storage

Единицей принятого состояния является кортеж `(plugin_id, code revision,
artifact digest, lock revision, storage revision)`. Нельзя помечать update
принятым, очищать restore point или запускать предыдущий code revision отдельно
от соответствующего storage state.

#### 7.2.1. Cross-runtime fencing barrier

`serve` с embedded worker и split web/worker deployment могут держать несколько
storage ports одного `plugin_id` в одном или разных процессах. Поэтому update и
rollback координируются не process-local lock, а shared durable host-owned
записью, доступной всем runtimes этого storage backend:

- монотонный `update_epoch`, lifecycle-state update и active storage revision;
- уникальные runtime instance IDs, runtime mode и leases всех instances,
  которым выдавался storage port текущей revision;
- fencing token каждого port: `(plugin_id, update_epoch, storage_revision,
  runtime_instance_id)` и heartbeat/lease deadline;
- barrier acknowledgements для нового epoch.

Authoritative fencing record хранится в core-owned области **того же storage
backend и transactional domain**, что и plugin tables. Он не входит в plugin
snapshot и не откатывается вместе с ним. Внешний coordinator может зеркалить
epoch для уведомлений и recovery, но не может авторизовать storage operation:
отдельный durable store без distributed transaction оставляет TOCTOU между
проверкой token и commit plugin write.

Перед migration, restore или переключением storage pointer хост открывает
storage-local write transaction, выполняет compare-and-swap fencing record из
ожидаемых `active/epoch/revision` в `fencing/new_epoch`, запрещает новые ports и
только затем commit-ит barrier. Новый epoch публикуется другим runtimes **после**
этого commit.

Каждый mutating вызов storage port обязан либо быть одним conditional write с
predicate по fencing record, либо в одной write transaction сначала получить
тот же storage-local serialization barrier, проверить token/state/revision,
выполнить все изменения plugin tables и commit. Раздельные «check, затем write»,
in-memory cache и проверка во внешнем store запрещены. Поэтому существует один
порядок событий: write, уже владевший barrier, полностью commit-ится до fence и
попадает в restore point; fence, commit-ившийся первым, заставляет stale write
abort без изменения данных. Read и schema operations проверяют тот же epoch в
своей storage transaction.

Backend, который не умеет атомарно сериализовать fencing CAS и plugin write, не
может использовать shared writable plugin storage. Online full-database-file
restore также не является допустимым plugin rollback: он заменил бы
authoritative fencing record. Обычный rollback восстанавливает только plugin
namespace/shadow revision, сохраняя core fencing rows. Полное восстановление
файла БД допустимо только как отдельная offline disaster-recovery процедура
после остановки всех runtimes и закрытия всех соединений; оно не участвует в
update acceptance и не полагается на plugin fencing protocol.

Старый token после commit storage-local fence больше не может читать, писать
или менять schema, даже если процесс потерял связь с coordinator.

Хост уведомляет все известные runtime leases, останавливает их hooks/tasks,
закрывает ports и собирает acknowledgement нового epoch. Barrier считается
достигнутым, когда все holders подтвердили fence либо их leases истекли, а
storage backend доказуемо отклоняет их stale tokens. Список holders включает не
только ожидаемые runtime targets update, но и любую ещё живую instance, которой
выдавался port этой plugin/storage revision. Не ответивший runtime остаётся
fenced и не может самостоятельно продолжить работу после восстановления.

Только в состоянии `fenced` хост может перейти в `restoring` и менять plugin
storage. После атомарного restore/switch coordinator публикует предыдущую
storage revision в новом epoch. Каждый runtime заново сверяет code/storage
revision и получает новый port; старые tokens не переактивируются. Состояния
`fencing` и `restoring` durable и восстанавливаемы после crash coordinator:
startup продолжает barrier/rollback и не выдаёт ports fail-open.

При failure на migration, `initialize`, health или acceptance rollback идёт в
следующем порядке:

1. Хост запускает barrier из раздела 7.2.1, отзывает hooks и storage ports во
   всех runtime holders и останавливает все instances непринятой ревизии.
2. Проверяет provenance и целостность сохранённого restore point. Для shadow
   storage достаточно отбросить непринятую revision; для snapshot-модели хост
   только после полного fence в эксклюзивной транзакции восстанавливает прежние
   схему и данные.
3. В одной storage transaction возвращает указатель на предыдущую revision и
   переводит fencing record в `active/new_epoch`. После commit durable
   activation record связывает её с previous code и lock revision, и только
   затем epoch публикуется runtimes. Частично восстановленный кортеж не может
   получить ports или стать active.
4. Проверяет schema/data revision и только затем инициализирует предыдущую
   версию плагина во всех требуемых runtime targets с новыми ports. Если fence,
   restore или проверка не удались, плагин остаётся fail-closed в состоянии
   `rollback_failed`; старый код не запускается против неизвестной схемы.
5. После старта предыдущей версии хост повторяет проверку обязательных hooks и
   plugin-specific `health()`, фиксируя результат rollback в audit.

Restore point хранится как минимум до атомарной записи успешного acceptance и
истечения rollback retention policy. Он шифруется и защищается теми же правами,
что и исходная БД, поскольку может содержать приватные данные. Его digest,
storage revision, время создания и причина удаления входят в audit.

Plugin rollback покрывает только namespaced storage, которым владеет плагин.
Изменения доменных таблиц ядра через versioned core operations должны быть
идемпотентными либо иметь отдельную host-owned compensating transaction; плагин
не может объявить их частью своего snapshot и самостоятельно откатывать.

## 8. Границы следующих подзадач

| Подзадача | Разрешённый результат после утверждения дизайна |
|---|---|
| 325.1 | Публичный interface, metadata-модели, registry и entry-point discovery. |
| 325.2 | Создание контекста и подключение lifecycle к `src/web/bootstrap.py`/worker startup и shutdown. |
| 325.3 | Policy для external/private Git, staging, pin/digest, credentials, validation и rollback. |
| 325.4 | Reference-плагин, пользовательская документация и тесты контракта/security policy. |

В 325.0 не меняются `pyproject.toml`, `src/`, database schema, runtime
конфигурация и зависимости. Этот PR содержит только решение и вопросы к
владельцу.

## 9. Вопросы на решение владельца

Предлагаемые значения отмечены как рекомендуемые; до решения они не считаются
реализуемой спецификацией.

1. **Trust boundary:** принять рекомендуемый in-process режим только для
   уровней A–C и явно отложить уровень D/out-of-process (рекомендуется).
2. **Entry point group:** принять `tg_agent.plugins` как единственный механизм
   package discovery, без filesystem scanning (рекомендуется).
3. **Database port:** разрешить только versioned domain operations плюс
   namespaced plugin storage; не передавать raw `Database` и SQL connection
   (рекомендуется).
4. **Telegram permissions:** разделить `telegram.read` и
   `telegram.send/publish`, направляя irreversible operations через общий
   audit/confirmation path (рекомендуется).
5. **Failure policy:** optional plugin по умолчанию отключается без отказа
   startup; `required` — явное свойство deployment policy (рекомендуется).
6. **Compatibility baseline:** ввести отдельный `core_plugin_api` и версии
   ports; не привязывать совместимость к `tg-agent` release version
   (рекомендуется).
7. **Supply chain:** обязательны allowlist, immutable commit/artifact digest,
   staging и ручное одобрение; auto-update запрещён (рекомендуется).
