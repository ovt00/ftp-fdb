# -*- coding: utf-8 -*-
# модуль работы с FTP, FDB

from ftplib import FTP
from os import path
import os
import fdb

# Получение сессии FTP
# ftpserver - кортеж с
# ftpname, ftpsite, ftpport, ftpuser, ftppassword – название, FTP-сервер , ftp-порт, логин/пароль для входа
def sessionftp(ftpserver):
    ftpname, ftpsite, ftpport, ftpuser, ftppassword = ftpserver
    session = FTP()
    print('    Устанавливается соединение с {0} ...'.format(ftpname))
    try:
        session.connect(ftpsite, ftpport, 20)
    except Exception as e:
        print('    Ошибка соединения с {0}!({1})'.format(ftpname, e))
        session.close()
        return None
    try:
        session.login(ftpuser, ftppassword)
    except Exception as e:
        print('    Ошибка аутинтификации с {0}!({1})'.format(ftpname, e))
        session.close()
        return None
    return session

# Загрузка файла с FTP-сервера:
# ftpdir - папка на сервере
# ftpfile – отправляемый файл
# для загрузки файла используется текущая папка
def getftp(session, ftpdir, ftpfile):
    print('    Получение файла {0} с сервера...'.format(ftpfile))
    session.cwd('/')
    session.cwd(ftpdir)
    try:
        f = open(ftpfile, 'wb')
        session.retrbinary('RETR ' + ftpfile, f.write)
        f.close()
    except Exception as e:
        f.close()
        os.unlink(ftpfile)
        session.close()
        print('    Ошибка получения файла {0}!({1}'.format(ftpfile, e))
        return False
    return True

# Закрытие соединения с ftp-сервером
def closeftp(session):
    print('    Закрытие соединения ...')
    try:
        session.close()
    except:
        print('    Ошибка закрытие соединения!')
        return False
    return True

# Соединение с firebird БД:
# dadabase – БД
# user, password - логин/пароль к БД
def connectfbd(database, user, password):
    print('    Устанавливается соединение с БД {0} ...'.format(database))
    try:
        connect = fdb.connect(dsn=database, user = user, password = password)
        cur = connect.cursor()
    except Exception as e:
        print('    Ошибка соединения с БД {0}!({1}'.format(database, e))
        connect.close()
        return None, None
    return connect, cur

# Сравнение данных
# execute_e - select
# checkdata - кортеж значений
def checkdatatable(connect, cur, execute_e, checkdata):
    # получаем данные с таблицы
    cur = getexecute(connect, cur, execute_e)
    if not cur:
        return False
    realdata = cur.fetchall()
    print('    Проверяем значения в БД...')
    if realdata == checkdata:
        print('    В БД значения верны.')
        return False
    else:
        print('    В БД значения неверны!({0} и {1}'.format(realdata, checkdata))
        return True

# Выполнение sql-запрос
def getexecute(connect, cur, execute_e):
    print('    Выполняем SQL-запрос к БД ...')
    try:
        cur.execute(execute_e)
    except Exception as e:
        print('    Ошибка SQL-запроса!({0}'.format(e))
        connect.close()
        return None
    return cur

# Выполнение commit
def getcommit(connect):
    print('    Выполняем commit к БД ...')
    try:
        connect.commit()
    except Exception as e:
        print('    Ошибка выполнения commit!')
        connect.close()
        return False
    return True

# Закрытие соединения с БД
def closefdb(connect):
    print('    Закрытие соединения с БД...')
    connect.close()
    return True

# Переименовываем файл на ftp-сервере
# ftpoldfile, ftpnewfile – старое/новое имя файла
def renameftp(session, ftpoldfile, ftpnewfile):
    print('    Переименовываем {0} в {1}...'.format(ftpoldfile, ftpnewfile))
    session. cwd('/')
    try:
        session.sendcmd('DELE ' + ftpnewfile)
    except :
        pass
    try:
        session.rename(ftpoldfile, ftpnewfile)
    except Exception as e:
        print('    Ошибка переименования файла {0}!({1})'.format(ftpoldfile, ftpnewfile))
        session.close()
        return False
    return True

# Загрузка файла по FTP на сервер:
# localfile – отправляемый файл
# ftpdir – папка на FTP-сервере для загрузки файла
def putftp(session, localfile, ftpdir):
    print('    Отправка файла {0} на сервер...'.format(localfile))
    try:
        session.cwd(ftpdir)
        f = open(localfile, 'rb')
        session.storbinary("STOR " + localfile, f)
        f.close()
    except Exception as e:
        print('    Ошибка отправки файла {0}!({1}'.format(localfile, e))
        session.close()
        return False
    return True

# Сравнение размера файла на FTP-сервере и на локальном диске
def checksizefileftp(session, localfile, ftpfile):
    print('    Сравниваем размер {0} с файлом на сервере {1}...'.format(localfile, ftpfile))
    ftpsize = session.size(ftpfile)
    localsize = path.getsize(localfile)
    if ftpsize == localsize:
        print('    Файлы равны.')
    else:
        print('    Файлы отличаются!')