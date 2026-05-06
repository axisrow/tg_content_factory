from __future__ import annotations

from typing import Annotated

PHONE_ARG = Annotated[str, "Номер телефона аккаунта (например +79001234567)"]
CONFIRM_ARG = Annotated[bool, "Установите true для подтверждения действия"]
PHOTO_TARGET_ARG = Annotated[str, "ID диалога-получателя (из list_photo_dialogs или me)"]
FILE_PATHS_ARG = Annotated[str, "Пути к файлам через запятую (серверные пути)"]
MODE_ARG = Annotated[str, "Режим отправки: album или separate"]
CAPTION_ARG = Annotated[str, "Подпись к фото"]
AUTO_JOB_ID_ARG = Annotated[int, "ID автозагрузки из list_auto_uploads"]

LIST_PHOTO_BATCHES_SCHEMA = {"limit": Annotated[int, "Максимальное количество результатов"]}
LIST_PHOTO_ITEMS_SCHEMA = {"limit": Annotated[int, "Максимальное количество результатов"]}
LIST_AUTO_UPLOADS_SCHEMA: dict[str, object] = {}
LIST_PHOTO_DIALOGS_SCHEMA = {"phone": PHONE_ARG}
REFRESH_PHOTO_DIALOGS_SCHEMA = {"phone": PHONE_ARG, "confirm": CONFIRM_ARG}

SEND_PHOTOS_NOW_SCHEMA = {
    "phone": PHONE_ARG,
    "target": PHOTO_TARGET_ARG,
    "file_paths": FILE_PATHS_ARG,
    "mode": MODE_ARG,
    "caption": CAPTION_ARG,
    "confirm": CONFIRM_ARG,
}

SCHEDULE_PHOTOS_SCHEMA = {
    "phone": PHONE_ARG,
    "target": PHOTO_TARGET_ARG,
    "file_paths": FILE_PATHS_ARG,
    "schedule_at": Annotated[str, "Дата/время отправки в формате ISO (YYYY-MM-DDTHH:MM:SS)"],
    "mode": MODE_ARG,
    "caption": CAPTION_ARG,
    "confirm": CONFIRM_ARG,
}

CANCEL_PHOTO_ITEM_SCHEMA = {
    "item_id": Annotated[int, "ID элемента из list_photo_items"],
    "confirm": CONFIRM_ARG,
}

CREATE_PHOTO_BATCH_SCHEMA = {
    "phone": PHONE_ARG,
    "target": PHOTO_TARGET_ARG,
    "file_paths": FILE_PATHS_ARG,
    "caption": CAPTION_ARG,
    "confirm": CONFIRM_ARG,
}

RUN_PHOTO_DUE_SCHEMA = {"confirm": CONFIRM_ARG}
TOGGLE_AUTO_UPLOAD_SCHEMA = {"job_id": AUTO_JOB_ID_ARG}
DELETE_AUTO_UPLOAD_SCHEMA = {"job_id": AUTO_JOB_ID_ARG, "confirm": CONFIRM_ARG}

CREATE_AUTO_UPLOAD_SCHEMA = {
    "phone": PHONE_ARG,
    "target": PHOTO_TARGET_ARG,
    "folder_path": Annotated[str, "Путь к папке на сервере"],
    "interval_minutes": Annotated[int, "Интервал автозагрузки в минутах"],
    "mode": MODE_ARG,
    "caption": CAPTION_ARG,
    "confirm": CONFIRM_ARG,
}

UPDATE_AUTO_UPLOAD_SCHEMA = {
    "job_id": AUTO_JOB_ID_ARG,
    "folder_path": Annotated[str, "Путь к папке на сервере"],
    "mode": MODE_ARG,
    "caption": CAPTION_ARG,
    "interval_minutes": Annotated[int, "Интервал автозагрузки в минутах"],
    "is_active": Annotated[bool, "Активна ли автозагрузка"],
    "confirm": CONFIRM_ARG,
}
