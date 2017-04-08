# -*- coding: utf-8 -*-
#from ftplib import FTP
import somedef

#-----------------------------------------------------
# Список FTP-серверов. Формат записи:
# (FTP-сервер, порт сервера, логин, пароль)
ftpservers = [
    #('Name', 'IP adress', 21, 'user', 'password'),

]

FTPFILE = 'XXXX'
FTPCOPYFILE = 'XXXX_'
FTPDIR = 'XXXX/XXXX/'
FTPDOWNLOADFILE = 'XXXX'
EXECUTE1 = 'UPDATE "table" SET "field" = CASE WHEN "value" = a1 THEN b1 WHEN "value" = a2 THEN b2 END'
EXECUTE2 = 'SELECT * FROM "table"'
CHECKDATA = [(1, 150000.0), (2, 150000.0)]

# перебор FTP-серверов
for ftpserver in ftpservers:
    print('Начало работы с {0} ...'.format(ftpserver[0]))

    # Данная последовательность вызовов функций обеспечивает:
    # получение файла с FTP-сервера; сравнение полей полученного файла с образцом; изменение полей таблицы при необходимости;
    # создание копии файла на FTP-сервере; загрузка файла на сервер
    # В итоге мы получаем на FTP-сервере файл с откорретированными значениями 
    # Меняя последовательность вызовов функций и комментируя не нужные, можно добиться решение других задач 


    # создаем сессию FTP
    session = somedef.sessionftp(ftpserver)
    if not session: continue

    # сравниваем файлы
    #if somedef.checksizefileftp(session, FTPDOWNLOADFILE, FTPFILE): continue

    # получаем файл с сервера
    if not somedef.getftp(session, FTPDIR, FTPDOWNLOADFILE): continue

    # закрываем сессию FTP
    somedef.closeftp(session)

    # создаем соединение с БД
    connect, cur = somedef.connectfbd(FTPDOWNLOADFILE, 'sysdba', 'masterkey')
    if not connect: continue

    # сравниваем значения в таблицу
    if not somedef.checkdatatable(connect, cur, EXECUTE2, CHECKDATA): continue

    # Изменяем таблицу
    if not somedef.getexecute(connect, cur, EXECUTE1): continue

    # запоминаем изменения в таблице
    if not somedef.getcommit(connect): continue

    # закрываем FDB
    somedef.closefdb(connect)

    # создаем сессию FTP
    session = somedef.sessionftp(ftpserver)
    if not session: continue

    # сохраняем текущий файл как копию, если копия ужа была - перезаписываем, если не было исходного файла - ничего не делаем
    if not somedef.renameftp(session, FTPFILE, FTPCOPYFILE): continue

    # заливаем файл на FTP-сервер
    if not somedef.putftp(session, FTPDOWNLOADFILE, FTPDIR): continue

    # закрываем сессию FTP
    somedef.closeftp(session)



