import os
import telebot
import sqlite3
import csv
from dotenv import load_dotenv
from datetime import datetime, timezone
from telebot.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)

# Загрузка переменных окружения из keys.env
load_dotenv(dotenv_path='keys.env')
BOT_TOKEN           = os.getenv('BOT_TOKEN')
ADMIN_USER_IDS      = [int(x) for x in os.getenv('ADMIN_USER_IDS','').split(',') if x]
SECRET_START_TOKEN  = os.getenv('SECRET_START_TOKEN', '452700')
REPORT_DIR          = 'reports'

# Создадим папку для отчётов, если её нет
os.makedirs(REPORT_DIR, exist_ok=True)

# Инициализация бота и базы
bot  = telebot.TeleBot(BOT_TOKEN)
conn = sqlite3.connect('botdata.db', check_same_thread=False)
c    = conn.cursor()

# Создание таблиц
c.execute("""CREATE TABLE IF NOT EXISTS subscribers (
    user_id    INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name  TEXT
)""")
c.execute("""CREATE TABLE IF NOT EXISTS allowed_users (
    user_id INTEGER PRIMARY KEY
)""")
c.execute("""CREATE TABLE IF NOT EXISTS group_list (
    group_name TEXT PRIMARY KEY
)""")
c.execute("""CREATE TABLE IF NOT EXISTS groups (
    group_name TEXT,
    user_id    INTEGER,
    UNIQUE(group_name, user_id)
)""")
c.execute("""CREATE TABLE IF NOT EXISTS messages (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT,
    text       TEXT,
    sent_time  TEXT
)""")
c.execute("""CREATE TABLE IF NOT EXISTS delivery (
    message_id INTEGER,
    user_id    INTEGER,
    delivered  BOOLEAN,
    PRIMARY KEY(message_id, user_id)
)""")
c.execute("""CREATE TABLE IF NOT EXISTS read_receipts (
    message_id INTEGER,
    user_id    INTEGER,
    read_time  TEXT,
    PRIMARY KEY(message_id, user_id)
)""")
conn.commit()

# — Утилиты работы с таблицей подписчиков и групп —
def add_subscriber(uid, fn, ln):
    c.execute("INSERT OR IGNORE INTO subscribers VALUES (?, ?, ?)", (uid, fn, ln))
    conn.commit()

def get_subscribers_info():
    c.execute("SELECT user_id, first_name, last_name FROM subscribers")
    return c.fetchall()

def allow_user(uid):
    c.execute("INSERT OR IGNORE INTO allowed_users VALUES (?)", (uid,))
    conn.commit()

def is_allowed(uid):
    c.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (uid,))
    return c.fetchone() is not None

def add_group(group_name):
    c.execute("INSERT OR IGNORE INTO group_list VALUES (?)", (group_name,))
    conn.commit()

def delete_group(group_name):
    c.execute("DELETE FROM group_list WHERE group_name = ?", (group_name,))
    c.execute("DELETE FROM groups     WHERE group_name = ?", (group_name,))
    conn.commit()

def get_all_groups():
    c.execute("SELECT group_name FROM group_list")
    return [r[0] for r in c.fetchall()]

def assign_user_to_group(uid, group):
    c.execute("INSERT OR IGNORE INTO groups VALUES (?, ?)", (group, uid))
    conn.commit()

def remove_user_from_group(uid, group):
    c.execute("DELETE FROM groups WHERE user_id = ? AND group_name = ?", (uid, group))
    conn.commit()

def get_group_info(group):
    c.execute("""
      SELECT s.user_id, s.first_name, s.last_name
        FROM subscribers s
        JOIN groups      g ON s.user_id = g.user_id
       WHERE g.group_name = ?
    """, (group,))
    return c.fetchall()

def remove_subscriber(uid):
    c.execute("DELETE FROM subscribers WHERE user_id = ?", (uid,))
    c.execute("DELETE FROM groups      WHERE user_id = ?", (uid,))
    conn.commit()

# — Утилиты отчёта —
def record_message(group, text):
    ts = datetime.now(timezone.utc).strftime("%d-%m-%Y %H:%M:%S")
    c.execute("INSERT INTO messages(group_name, text, sent_time) VALUES (?, ?, ?)", (group, text, ts))
    conn.commit()
    return c.lastrowid

def record_delivery(message_id, uid, success):
    c.execute("INSERT OR REPLACE INTO delivery VALUES (?, ?, ?)", (message_id, uid, success))
    conn.commit()

def record_read(message_id, uid):
    ts = datetime.now(timezone.utc).strftime("%d-%m-%Y %H:%M:%S")
    c.execute("INSERT OR IGNORE INTO read_receipts VALUES (?, ?, ?)", (message_id, uid, ts))
    conn.commit()

# — Рассылка с кнопкой «Я прочитал» —
def send_group_notification(group, text):
    mid = record_message(group, text)
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton('✅ Я прочитал', callback_data=f'ack_{mid}'))
    for uid, _, _ in get_group_info(group):
        try:
            bot.send_message(uid, f"Оповещение по списку «{group}»: {text}", reply_markup=kb)
            record_delivery(mid, uid, True)
        except:
            record_delivery(mid, uid, False)
    return mid

@bot.callback_query_handler(lambda cb: cb.data.startswith('ack_'))
def handle_ack(cb):
    mid = int(cb.data.split('_',1)[1])
    record_read(mid, cb.from_user.id)
    bot.answer_callback_query(cb.id, 'Отмечено как прочитанное')

# — Экспорт отчёта в CSV —
@bot.message_handler(commands=['export_report'])
def cmd_export_report(m):
    if m.from_user.id not in ADMIN_USER_IDS:
        return bot.send_message(m.chat.id, 'Нет прав.')
    parts = m.text.split(maxsplit=1)
    if len(parts)!=2 or not parts[1].isdigit():
        return bot.send_message(m.chat.id, 'Используй: /export_report <message_id>')
    mid = int(parts[1])
    fname = os.path.join(REPORT_DIR, f'report_{mid}.csv')
    with open(fname, 'w', newline='', encoding='utf-8') as f:
        wr = csv.writer(f)
        wr.writerow(['user_id','delivered','read_time'])
        c.execute("""
          SELECT d.user_id, d.delivered, rr.read_time
            FROM delivery d
            LEFT JOIN read_receipts rr
              ON d.message_id=rr.message_id AND d.user_id=rr.user_id
           WHERE d.message_id=?
        """, (mid,))
        for row in c.fetchall(): wr.writerow(row)
    bot.send_document(m.chat.id, open(fname, 'rb'))

# — /start и /subscribe с проверкой по ссылке —
@bot.message_handler(commands=['start'])
def cmd_start(m):
    parts = m.text.split(maxsplit=1)
    if len(parts)==2 and parts[1]==SECRET_START_TOKEN:
        allow_user(m.from_user.id)
        bot.send_message(m.chat.id, "Доступ открыт. Теперь выполните /subscribe.")
    else:
        bot.send_message(m.chat.id,
            f"Подписаться можно только по прямой ссылке:\n"
            f"https://t.me/{bot.get_me().username}?start={SECRET_START_TOKEN}"
        )

@bot.message_handler(commands=['subscribe'])
def cmd_subscribe(m):
    uid = m.from_user.id
    if not is_allowed(uid):
        return bot.send_message(m.chat.id,
            f"Подписаться только по ссылке:\n"
            f"https://t.me/{bot.get_me().username}?start={SECRET_START_TOKEN}"
        )
    fn, ln = m.from_user.first_name, m.from_user.last_name or ''
    add_subscriber(uid, fn, ln)
    bot.send_message(m.chat.id, f"Вы подписаны, {fn} {ln}!")

# — Группы через команды —
@bot.message_handler(commands=['add_group'])
def cmd_add_group(m):
    if m.from_user.id not in ADMIN_USER_IDS: return bot.send_message(m.chat.id, 'Нет прав.')
    parts = m.text.split(maxsplit=1)
    if len(parts)!=2: return bot.send_message(m.chat.id, 'Используй: /add_group <group>')
    add_group(parts[1])
    bot.send_message(m.chat.id, f"Группа «{parts[1]}» создана.")

@bot.message_handler(commands=['delete_group'])
def cmd_delete_group(m):
    if m.from_user.id not in ADMIN_USER_IDS: return bot.send_message(m.chat.id, 'Нет прав.')
    parts = m.text.split(maxsplit=1)
    if len(parts)!=2: return bot.send_message(m.chat.id, 'Используй: /delete_group <group>')
    delete_group(parts[1])
    bot.send_message(m.chat.id, f"Группа «{parts[1]}» удалена.")

# — Админ‑меню —
def admin_menu(chat_id):
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton('📋 Подписчики', callback_data='menu_list_subs'), InlineKeyboardButton('📂 Обзор групп', callback_data='menu_list_groups'))
    kb.row(InlineKeyboardButton('➕ В группу', callback_data='menu_assign'), InlineKeyboardButton('➖ Из группы', callback_data='menu_remove_group'))
    kb.row(InlineKeyboardButton('📇 Импорт контакта', callback_data='menu_import_contact'))
    kb.row(InlineKeyboardButton('➕ Создать группу', callback_data='menu_create_group'), InlineKeyboardButton('🗑️ Удалить группу', callback_data='menu_delete_group'))
    kb.row(InlineKeyboardButton('✉️ Всем', callback_data='menu_notify_all'), InlineKeyboardButton('✉️ Группе', callback_data='menu_notify_group'))
    bot.send_message(chat_id, 'Админ‑меню:', reply_markup=kb)

@bot.message_handler(commands=['menu'])
def cmd_menu(m):
    if m.from_user.id not in ADMIN_USER_IDS: return bot.send_message(m.chat.id, 'Нет прав.')
    admin_menu(m.chat.id)

# — Обработчик меню —
@bot.callback_query_handler(lambda c: c.data.startswith('menu_'))
def process_menu(c):
    data = c.data; cid = c.message.chat.id
    bot.answer_callback_query(c.id)
    if c.from_user.id not in ADMIN_USER_IDS: return bot.send_message(cid,'Нет прав.')

    if data == 'menu_list_subs':
        rows = get_subscribers_info(); text='📋 Подписчики:\n' + ''.join(f'• {u}: {n} {l}\n' for u,n,l in rows)
        bot.send_message(cid, text or 'Нет подписчиков.')

    elif data == 'menu_list_groups':
        gs = get_all_groups();
        if not gs: return bot.send_message(cid,'Нет групп.')
        text='📂 Обзор групп:\n'
        for g in gs:
            ms = get_group_info(g); text+=f'\n🔹 {g} ({len(ms)})\n' + ''.join(f'   • {uid}: {fn} {ln}\n' for uid,fn,ln in ms)
        bot.send_message(cid, text)

    elif data == 'menu_assign':
        msg=bot.send_message(cid,'Введите: <user_id> <group>')
        bot.register_next_step_handler(msg,handle_assign)

    elif data == 'menu_remove_group':
        msg=bot.send_message(cid,'Введите: <user_id> <group>')
        bot.register_next_step_handler(msg,handle_remove_group)

    elif data == 'menu_import_contact':
        kb=ReplyKeyboardMarkup(resize_keyboard=True,one_time_keyboard=True)
        kb.add(KeyboardButton('📲 Поделиться контактом',request_contact=True))
        bot.send_message(cid,'Выберите контакт для импорта:',reply_markup=kb)

    elif data == 'menu_create_group':
        msg=bot.send_message(cid,'Введите имя новой группы:')
        bot.register_next_step_handler(msg,handle_create_group)

    elif data == 'menu_delete_group':
        gs=get_all_groups();
        msg=bot.send_message(cid,'Группы:\n'+"\n".join(f'- {g}' for g in gs)+"\nВведите имя для удаления:")
        bot.register_next_step_handler(msg,handle_delete_group)

    elif data == 'menu_notify_all':
        msg=bot.send_message(cid,'Введите текст для рассылки всем:')
        bot.register_next_step_handler(msg,handle_notify_all)

    elif data == 'menu_notify_group':
        gs=get_all_groups();
        msg=bot.send_message(cid,'Группы:\n'+"\n".join(f'- {g}' for g in gs)+"\nФормат: <group> <text>")
        bot.register_next_step_handler(msg,handle_notify_group)

    else:
        bot.send_message(cid,'Неизвестная команда.')

# — Шаговые хендлеры меню —
def handle_assign(m):
    try: uid,grp=m.text.split(maxsplit=1); assign_user_to_group(int(uid),grp); bot.send_message(m.chat.id,'Готово.')
    except: bot.send_message(m.chat.id,'Ошибка формата.')

def handle_remove_group(m):
    try: uid,grp=m.text.split(maxsplit=1); remove_user_from_group(int(uid),grp); bot.send_message(m.chat.id,'Готово.')
    except: bot.send_message(m.chat.id,'Ошибка формата.')

def handle_create_group(m):
    grp=m.text.strip()
    if not grp: return bot.send_message(m.chat.id,'Имя не может быть пустым.')
    add_group(grp); bot.send_message(m.chat.id,f'Группа «{grp}» создана.')

def handle_delete_group(m):
    grp=m.text.strip()
    if not grp: return bot.send_message(m.chat.id,'Имя не может быть пустым.')
    delete_group(grp); bot.send_message(m.chat.id,f'Группа «{grp}» удалена.')

def handle_notify_all(m):
    for uid,_,_ in get_subscribers_info():
        try: bot.send_message(uid,m.text)
        except: pass
    bot.send_message(m.chat.id,'Разослано.')

def handle_notify_group(m):
    try:
        grp,txt=m.text.split(maxsplit=1)
        send_group_notification(grp,txt)
        bot.send_message(m.chat.id,'Разослано.')
    except:
        bot.send_message(m.chat.id,'Ошибка формата.')

# — Обработчик присланного контакта —
@bot.message_handler(content_types=['contact'])
def handle_contact(message):
    ct=message.contact
    if ct.user_id:
        allow_user(ct.user_id)
        add_subscriber(ct.user_id,ct.first_name or'',ct.last_name or'')
        bot.send_message(message.chat.id,f'Импорт: {ct.first_name} {ct.last_name}',reply_markup=ReplyKeyboardRemove())
    else:
        bot.send_message(message.chat.id,'Контакт без Telegram ID.',reply_markup=ReplyKeyboardRemove())

# Запуск поллинга
bot.polling(none_stop=True)
