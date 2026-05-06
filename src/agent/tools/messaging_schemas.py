from __future__ import annotations

from typing import Annotated

PHONE_ARG = Annotated[str, "Номер телефона аккаунта (например +79001234567)"]
CHAT_ID_ARG = Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"]
CONFIRM_ARG = Annotated[bool, "Установите true для подтверждения действия"]

SEND_MESSAGE_SCHEMA = {
    "phone": PHONE_ARG,
    "recipient": Annotated[str, "Получатель (@username, телефон или числовой ID)"],
    "text": Annotated[str, "Текст сообщения"],
    "confirm": CONFIRM_ARG,
}

EDIT_MESSAGE_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "message_id": Annotated[int, "ID сообщения в Telegram"],
    "text": Annotated[str, "Текст сообщения"],
    "confirm": CONFIRM_ARG,
}

DELETE_MESSAGE_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "message_ids": Annotated[str, "ID сообщений через запятую"],
    "confirm": CONFIRM_ARG,
}

FORWARD_MESSAGES_SCHEMA = {
    "phone": PHONE_ARG,
    "from_chat": Annotated[str, "ID чата-источника (@username, числовой ID)"],
    "to_chat": Annotated[str, "ID чата-получателя (@username, числовой ID)"],
    "message_ids": Annotated[str, "ID сообщений через запятую"],
    "confirm": CONFIRM_ARG,
}

PIN_MESSAGE_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "message_id": Annotated[int, "ID сообщения в Telegram"],
    "notify": Annotated[bool, "Отправить уведомление участникам"],
    "confirm": CONFIRM_ARG,
}

UNPIN_MESSAGE_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "message_id": Annotated[int, "ID сообщения в Telegram"],
    "confirm": CONFIRM_ARG,
}

DOWNLOAD_MEDIA_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "message_id": Annotated[int, "ID сообщения в Telegram"],
}

GET_PARTICIPANTS_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "limit": Annotated[int, "Максимальное количество результатов"],
    "search": Annotated[str, "Фильтр по имени/username участника"],
}

EDIT_ADMIN_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "user_id": Annotated[str, "ID пользователя (@username или числовой ID)"],
    "is_admin": Annotated[bool, "true — назначить админом, false — снять права"],
    "title": Annotated[str, "Кастомный бейдж администратора"],
    "confirm": CONFIRM_ARG,
}

EDIT_PERMISSIONS_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "user_id": Annotated[str, "ID пользователя (@username или числовой ID)"],
    "send_messages": Annotated[bool, "Разрешить отправку сообщений"],
    "send_media": Annotated[bool, "Разрешить отправку медиа"],
    "until_date": Annotated[str, "Дата окончания ограничения в формате ISO (YYYY-MM-DDTHH:MM:SS)"],
    "confirm": CONFIRM_ARG,
}

KICK_PARTICIPANT_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "user_id": Annotated[str, "ID пользователя (@username или числовой ID)"],
    "confirm": CONFIRM_ARG,
}

GET_BROADCAST_STATS_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
}

ARCHIVE_CHAT_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "confirm": CONFIRM_ARG,
}

UNARCHIVE_CHAT_SCHEMA = ARCHIVE_CHAT_SCHEMA

MARK_READ_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "max_id": Annotated[int, "Отметить прочитанными до этого ID включительно"],
}

READ_MESSAGES_SCHEMA = {
    "phone": PHONE_ARG,
    "chat_id": CHAT_ID_ARG,
    "limit": Annotated[int, "Максимальное количество результатов"],
}
