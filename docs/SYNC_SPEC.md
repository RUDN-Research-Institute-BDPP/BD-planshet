# SYNC_SPEC: отправка результатов Planshet на узел хранилища

Версия 1.0. Дополняет существующее приложение (index.html, IndexedDB, три теста: «Слова», «Мелкая моторика», «Рисовалка»). Ничего из текущего UX не ломать: офлайн-режим, локальное хранение и экспорт CSV/ZIP остаются как были (резервный канал).

## 1. Цель
Каждый завершённый тест автоматически уезжает на узел `https://storage.lan` без действий оператора; при отсутствии сети — копится и доезжает позже. Пациент выбирается из списка узла, чтобы канал не порождал конфликтов имён.

## 2. Конфигурация (новый экран «Настройки», хранится в IndexedDB)
`node_url` (по умолчанию https://storage.lan), `api_key`, `device_id` (генерится раз: `pl-<8hex>`), `operator_label` (свободный текст).
**Основной способ настройки — сканировать QR** с экрана админки узла (кнопка «Сканировать QR»: getUserMedia + вендоренный jsQR или BarcodeDetector API при наличии; QR содержит JSON `{"url":"...","key":"..."}`). Ручной ввод — fallback. Кнопка «Проверить связь» -> `GET {node_url}/api/ingest/status` c X-Api-Key (ожидать 200/401; сетевую ошибку из-за неустановленного TLS-сертификата показать человечески: «Установите сертификат узла — Настройки -> инструкция», ссылка на {node_url}/setup/cert).

## 3. Справочник пациентов с узла
- При онлайне раз в 15 мин и по кнопке: `GET {node_url}/api/persons?limit=500` (X-Api-Key) -> кэш в IndexedDB store `node_persons` {id, display_code, full_name?}.
- UI выбора пациента: поиск по кэшу узла (основной путь). «Нет в списке» -> локальный ввод имени как сейчас (fallback офлайна/новых людей) — запись помечается `patient_ref: {local_name}`.
- Выбор из узла -> `patient_ref: {node_person_id, display_code}`.
- К записи теста добавляются поля: `timepoint` (1|2|null — селектор «Точка», запоминается до смены пациента) и `condition` (null|'pre_load'|'post_load' — селектор «Нагрузка», по умолчанию null).

## 4. Очередь отправки (новый store `outbox`)
- По завершении теста: собрать батч по CONTRACT_INGEST §B storage-core (нормативен; копия структуры ниже) и положить в outbox со status='pending'.
- Фоновый цикл (при старте приложения, по таймеру 60 с, по событию online): pending-батчи отправляются по одному: `POST {node_url}/api/ingest/planshet`, multipart:
  - часть `manifest` (JSON): `{manifest_version:1, batch_id, source:"planshet", machine:<device_id>, exporter:{name:"planshet", version:<app_version>}, created_at, files:[{path,sha256,size}...], counts:{records:1}}`
  - часть `registry/records.jsonl`: одна строка: `{"uid":"planshet:<device_id>:<test_id>", "patient_ref":{...}, "test":"words"|"motor"|"draw", "timepoint":1|2|null, "condition":null|"pre_load"|"post_load", "operator":<operator_label>, "created_at":<ISO>, "payload":{— все поля теста как в текущих CSV-экспортах —}, "files":["files/audio_1.webm", ...]}`
  - части-файлы: аудио (words), png+траектория-CSV (draw) — пути = files[] манифеста.
- `batch_id` = `<UTC yyyymmddThhmmssZ>_<8 hex>` — СТРОГО общий формат §A (узел валидирует regexp `^\d{8}T\d{6}Z_[0-9a-z]{8}$`; формат с device_id/test_id в batch_id ЗАПРЕЩЁН — отвергнется). Детерминированность повторной отправки: batch_id генерируется ОДИН раз при постановке теста в outbox и хранится в записи outbox — все повторные попытки шлют тот же batch_id (узел ответит duplicate = УСПЕХ, пометить sent). device_id уходит в manifest.machine, test_id — в manifest.source_state.test_id.
- sha256 файлов — через WebCrypto (crypto.subtle.digest).
- Ответы: 202/200-duplicate -> status='sent' (+sent_at); 401/403 -> status='auth_error' (баннер «проверь ключ», не ретраить до смены настроек); сеть/5xx -> остаётся pending, ретрай следующим циклом (без экспоненты, цикл и так редкий).
- UI: бейдж в шапке «Не отправлено: N» + список в настройках (кнопка «отправить сейчас», «показать ошибку»).

## 5. Правила
- Данные в IndexedDB НЕ удаляются после отправки (локальная копия = резерв; чистка — вручную как сейчас).
- Никаких внешних CDN; всё по-прежнему в одном index.html (+sw.js). Совместимость: Chrome/WebView планшета.
- api_key в IndexedDB — приемлемо для внутреннего контура; в URL не подставлять, только заголовок.
- CORS: узел разрешает origin планшета (уже учтено на стороне узла; если увидишь CORS-ошибку в тестах против заглушки — фиксируй в PROGRESS, не выдумывай прокси).

## 6. Тест-план (без реального узла)
Мини-заглушка `tests/mock_node.py` (stdlib http.server): /api/persons -> 3 синтетических человека; /api/ingest/planshet -> валидирует multipart+манифест+sha256, пишет принятые батчи в папку, повтор batch_id -> {"status":"duplicate"}; режимы сбоев (--fail-next, --unauth). Сценарии: (1) онлайн: тест -> в папке заглушки корректный батч; (2) офлайн (заглушка вык