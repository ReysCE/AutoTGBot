import logging
import random
import re
from collections import UserDict
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Thread
from time import sleep
import sqlite3

import requests
from bs4 import BeautifulSoup
from telegram import Bot, File, ParseMode, TelegramError, Update
from telegram.ext import (CommandHandler, Filters, Handler, MessageHandler,
                          Updater)

# voice book bot token
TELEGRAM_BOT_TOKEN = ''
TELEGRAM_BOT_NAME = 'v2sexbot'
TS_INTERVAL1 = 60 * 60 * 24 * 14  # 14day
TS_INTERVAL2 = 60 * 10  # 10minute
TS_INTERVAL3 = 60 * 3  # 3minute
V2EX_URL = 'https://www.v2ex.com/t/381521'
V2EX_TOKEN_REGEX = re.compile(r'[0-9]{6}')
# V2EX_TOKEN_REGEX = re.compile(r'V2EX')

loggerHandler = logging.StreamHandler()
loggerHandler.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(loggerHandler)

db = sqlite3.connect('main.db')


def logi(msg, *args, **kwargs):
    logger.info(msg, *args, **kwargs)


def loge(msg, *args, **kwargs):
    logger.error(msg, *args, **kwargs)


def logd(msg, *args, **kwargs):
    logger.debug(msg, *args, **kwargs)


class Group(UserDict):
    gid = None
    title = None
    ts_addition = None

    v2ex_url = V2EX_URL
    ts_v2ex_check = None
    v2ex_bond = {}

    def __init__(self, gid, title):
        super().__init__()
        self.gid = gid
        self.title = title
        self.ts_addition = datetime.now(timezone.utc)

        # TODO every group had self fetch url
        # self.ts_v2ex_check = None
        # self.v2ex_bond = {}

    def __str__(self):
        return '%d(%s)' % (self.gid, self.title)

    @staticmethod
    def check():
        '''fetch url and update user tokens'''
        now = datetime.now(timezone.utc)
        if Group.ts_v2ex_check is None:
            Group.ts_v2ex_check = now
        elif (now - Group.ts_v2ex_check).total_seconds() > TS_INTERVAL3:
            Group.ts_v2ex_check = now
        else:
            # ignore less than 60s check
            return

        Group.v2ex_bond.clear()

        idx = 0
        while True:
            try:
                resp = requests.get(Group.v2ex_url, timeout=(5.0, 5.0))
            except Exception as e:
                loge('url check failed:%s', e)
                break

            if resp.status_code != 200:
                # loge('group %s check failed:v2ex %d', Group, resp.status_code)
                loge('url status code not 200:%d %s', resp.status_code, resp.text)
                break

            soup = BeautifulSoup(resp.text, 'lxml')
            for reply in soup.select('td > div.reply_content'):
                try:
                    parent = reply.parent
                    username = parent.contents[4].text
                    content = reply.text.strip()
                except Exception as e:
                    loge('select reply content failed:%e', e)
                    continue

                # TODO auto check it
                m = V2EX_TOKEN_REGEX.search(content)
                if m is None:
                    continue

                # check token alway existed warn
                token = m.group(0)
                Group.v2ex_bond[token] = username
                logd('find user %s token %s', username, token)

            # TODO next page num
            break

    def save(self):
        pass


class GroupUser(object):
    gid = None
    uid = None
    username = None
    token = None
    token_checked = False

    ts_addition = None
    ts_last_say = None

    v2ex_username = None

    def __init__(self, gid, uid, username):
        self.gid = gid
        self.uid = uid
        self.username = username
        self.usernamev2 = None
        self.token = str(random.randrange(100001, 999999))
        self.token_checked = False

        now = datetime.now(timezone.utc)
        self.ts_addition = now
        self.ts_last_say = now

        self.v2ex_username = None

    def __str__(self):
        return '%d(%s)' % (self.uid, self.username)

    def save(self):
        pass


class TelegramBot(Thread):

    updater = Updater(TELEGRAM_BOT_TOKEN)

    is_idle = False
    queue = None
    groups = {}

    def __init__(self):
        super().__init__()

        self.is_idle = False
        self.queue = Queue(maxsize=5000)
        self.groups = {}

        # DEBUG
        # o = self.updater.dispatcher.process_update

        # def n(update, *args, **kwargs):
        #     print(update)

        #     for group in self.updater.dispatcher.groups:
        #         for handler in self.updater.dispatcher.handlers[group]:
        #             if handler.check_update(update):
        #                 print(handler)
        #                 handler.handle_update(update, self.updater.dispatcher)
        #                 break

        #     o(update, *args, **kwargs)
        # self.updater.dispatcher.process_update = n

        self.updater.dispatcher.add_handler(CommandHandler('bind', self.cmd_bind), 500)
        self.updater.dispatcher.add_handler(CommandHandler('b', self.cmd_bind), 500)
        self.updater.dispatcher.add_handler(MessageHandler(Filters.group, self.msg_group), 1000)

    def run(self):
        self.updater.start_polling()
        self.updater.is_idle = True

        logi('telegram bot online')
        self.is_idle = True
        while self.is_idle:
            now = datetime.now(timezone.utc)

            groups_need_remove = []
            for gid, group in self.groups.items():
                ts_last = None

                group.check()

                users_need_removed = []
                for uid, user in group.items():
                    if not user.token_checked and (now - user.ts_addition).total_seconds() > TS_INTERVAL2:
                        users_need_removed.append(uid)
                        logi('group %s kick member %s check token %s failed', group, user, user.token)
                        continue

                    # check last say timestamp
                    if (now - user.ts_last_say).total_seconds() > TS_INTERVAL1:
                        users_need_removed.append(uid)
                        logi('group %s kick member %s last say at %s', group, user, user.ts_last_say)

                    # check token
                    elif not user.token_checked and user.token in Group.v2ex_bond:
                        user.v2ex_username = Group.v2ex_bond[user.token]
                        user.token_checked = True
                        logi('group %s member %s bond', group, user)

                        try:
                            self.updater.bot.send_message(user.uid,  'group %s bond' % group.title)
                        except:
                            pass

                    # normal user
                    else:
                        if ts_last is None:
                            ts_last = user.ts_last_say
                        elif user.ts_last_say > ts_last:
                            ts_last = user.ts_last_say

                for uid in set(users_need_removed):
                    group.pop(uid)
                    # TODO dont not remove admin

                    try:
                        admins = [m.user.id for m in self.updater.bot.get_chat_administrators(gid)]
                    except Exception as e:
                        loge('group %s get admin list failed:%s', group, e)
                        admins = []

                    if uid in admins:
                        user.ts_last_say = now
                        logd('group %s do not kick admin %s', group, user)
                    else:
                        if self.updater.bot.kick_chat_member(gid, uid):
                            logi('group %s kick member %s', group, user)
                            if not self.updater.bot.unban_chat_member(gid, uid):
                                loge('group %s unban member %s failed')
                        else:
                            loge('group %s kick member %s failed', group, user)

                # nobody say or dead group or empty group
                if ts_last is None and (now - group.ts_addition).total_seconds() > TS_INTERVAL2:
                    groups_need_remove.append(gid)
                if ts_last and (now - ts_last).total_seconds() > TS_INTERVAL1:
                    groups_need_remove.append(gid)

            for gid in groups_need_remove:
                self.groups.pop(gid)
                logi('group %d remove from pool', gid)

            if self.queue.empty():
                sleep(5)
                continue

            try:
                item = self.queue.get(block=False, timeout=2)
            except Empty:
                sleep(1)
                continue

            act, group, user, ts = item
            if group.gid not in self.groups:
                self.groups[group.gid] = group
            else:
                group = self.groups[group.gid]

            if act in ['up', 'add']:
                if user.uid not in group:
                    group[user.uid] = user
                    if act == 'up':
                        user.token_checked = True
                else:
                    user = group[user.uid]

                # update last say timestamp
                user.ts_last_say = ts

                if act == 'add':
                    if user.username.endswith('bot'):
                        # don not auto remove bot
                        user.token_checked = True
                        user.ts_last_say = datetime(3999, 1, 1, tzinfo=timezone.utc)
                        logi('group %s added bot %s', group, user)

                    else:
                        # TODO auto remove at 5min
                        try:
                            self.updater.bot.send_message(
                                group.gid,  'Welcome @%s\n#1 click @%s and Start\n#2 say /bind@%s in here' % (
                                    user.username, TELEGRAM_BOT_NAME, TELEGRAM_BOT_NAME
                                ))
                            logi('group %s user %s welcome', group, user)
                        except Exception as e:
                            logi('group %s user %s welcome send failed:%s', group, user, e)

            elif act == 'del':
                if user.uid in group:
                    del group[user.uid]

            elif act == 'bind':
                if user.uid not in group:
                    group[user.uid] = user
                    user.ts_addition = now
                else:
                    user = group[user.uid]

                user.token_checked = False
                user.ts_last_say = now

                if not user.token_checked:
                    try:
                        self.updater.bot.send_message(user.uid, 'Please send %s to [v2ex ticker](%s)' % (user.token, V2EX_URL), parse_mode=ParseMode.MARKDOWN)
                        logi('group %s user %s need push token %s', group, user, user.token)
                    except Exception as e:
                        logi('group %s user %s need push token %s send failed:%s', group, user, user.token, e)

                        try:
                            self.updater.bot.send_message(group.gid,  'Hi ~ @%s Start ME :D\nI will send binding token to your.' % user.username)
                        except:
                            pass

            logd('group %s user %s %s at %s', group, user, act, ts)

    def msg_group(self, bot, update):
        group = update.message.chat
        if group.all_members_are_administrators and group.type not in ['group', 'supergroup']:
            return

        user = update.message.from_user
        members = update.message.new_chat_members
        member_left = update.message.left_chat_member
        now = datetime.fromtimestamp(update.message.date.timestamp(), timezone.utc)

        if members:
            for member in members:
                if member.username.endswith('bot'):
                    continue

                logi('group %d(%s) new member %d(%s) by %d(%s)', group.id, group.title, member.id, member.username, user.id, user.username)
                self.queue.put_nowait(('add', Group(group.id, group.title), GroupUser(group.id, member.id, member.username), now))
        elif member_left:
            if user.id == member.id:
                logi('group %d(%s) member %d(%s) leave', group.id, group.title, member_left.id, member_left.username)
            else:
                logi('group %d(%s) kick member %d(%s) by %d(%s)', group.id, group.title, member_left.id, member_left.username, user.id, user.username)
            self.queue.put_nowait(('del', Group(group.id, group.title), GroupUser(group.id, member_left.id, member_left.username), now))
        else:
            logi('group %d(%s) member %d(%s) say one', group.id, group.title, user.id, user.username)
            self.queue.put_nowait(('up', Group(group.id, group.title), GroupUser(group.id, user.id, user.username), now))

        # print(user, update, *args, **kwargs)
        # print(update)

    def cmd_bind(self, bot, update):
        group = update.message.chat
        if group.all_members_are_administrators:
            return

        user = update.message.from_user
        # members = update.message.new_chat_members
        # member_left = update.message.left_chat_member
        now = datetime.fromtimestamp(update.message.date.timestamp(), timezone.utc)

        logi('group %d(%s) member %d(%s) need bind', group.id, group.title, user.id, user.username)
        self.queue.put_nowait(('bind', Group(group.id, group.title), GroupUser(group.id, user.id, user.username), now))


if __name__ == '__main__':
    TELEGRAM_BOT_TOKEN = sys.argv[1]
    bot = TelegramBot()
    bot.start()

    # g = Group(1, 'Demo')
    # g.check()
