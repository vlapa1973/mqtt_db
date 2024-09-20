#!/usr/bin/env python3

import os
import paho.mqtt.client as mqtt
import time
from pathlib import Path
from dotenv import load_dotenv
import sqlite3

load_dotenv()

folder_archive_name = str("data")

broker = os.getenv('BROKER')
port = int(os.getenv('PORT'))
topic = os.getenv('TOPIC')
client_id2 = os.getenv('CLIENT_ID2')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

timeIn = time.time()
fileNameTemp = ''
path1 = ''
my_file1 = ''
flagWrite = False


def file_print(data_string, data_data):
    """ Обработчик данных от сервера MQTT. Передаем: топик, данные """
    fileName = data_string[1: data_string.rfind("/")]

    global timeIn, fileNameTemp, path1, my_file1, folder_archive_name, flagWrite
    if (time.time() - timeIn) > 3:
        fileNameTemp = ''
        timeIn = time.time()
        flagWrite = True

    if fileNameTemp != fileName:
        if bool(path1 != '') & flagWrite:
            fileWrite(folder_archive_name, path1, my_file1)
            db_write(path1[: path1.find('.')], my_file1[my_file1.rfind('*') + 1:])
            flagWrite = False

        timeIn = time.time()
        my_file1 = ''

        fileNameTemp = fileName
        path1 = f"{fileNameTemp}.csv"
        my_file1 += f"{time.strftime('/%Y/%m/%d - %H:%M:%S', time.localtime())}_*{data_data}"

    else:
        my_file1 += f",{data_data}"


def connect_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(username, password)
    client.connect(broker, port)
    return client


def subscribe(client: mqtt):
    def on_message(client, userdata, msg):
        try:
            topicNew: str = ""
            if len(msg.topic) < 24:
                topicNew = msg.topic + "\t\t"
            elif len(msg.topic) < 32:
                topicNew = msg.topic + "\t"

            file_print(msg.topic, msg.payload.decode())
        except Exception as error:
            print(error)

    client.subscribe(topic)
    client.on_message = on_message


def fileWrite(folderArhiveName, path, myFile):
    """ Запись в файл. Передаем: название папки архива, название файла (топика), данные """
    file_name_path = folderArhiveName + myFile[:myFile.find(' ')]

    pathTemp = f"{Path.cwd()}/{file_name_path}"

    if not Path(pathTemp).exists():
        Path(pathTemp).mkdir(parents=True, exist_ok=True)

    my_file = open(f"{pathTemp}/{path}", "a")
    my_file.write(myFile[myFile.rfind(' '):] + '\n')
    my_file.close()
    print('\n---------------')
    print(myFile)
    print('---------------')
    print('my_File= ', path)


def db_write(topic, data_text):
    print('topic= ', topic)
    print('data = ', data_text)

    try:
        with (sqlite3.connect('file:sensors.db?mode=rw', uri=True)) as conn:
            cursor = conn.cursor()
            print('sensors.db is OK !')
    except:
        with (sqlite3.connect('sensors.db')) as conn:
            cursor = conn.cursor()
            query = """CREATE TABLE sensors (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        loc TEXT(32) NOT NULL,
                        name TEXT(32) NOT NULL,
                        date TEXT(10) NOT NULL,
                        times TEXT(8) NOT NULL,
                        temp TEXT(8) NOT NULL,
                        pres TEXT(8) NOT NULL,
                        hum TEXT(3) NOT NULL,
                        vcc TEXT(8) NOT NULL,
                        other TEXT(255)
                        )"""
            cursor.execute(query)
            print('sensors.db is NEW table !')
    finally:
        pass

    # with (sqlite3.connect('sensors.db')) as conn:
    #         cursor = conn.cursor()

    text1 = data_text.split(',')

    # GasBoiler_Villa
    # text1 = [ON,21.06,23.00,0.00,4:06:13:56,___,___]
    if len(text1) == 7:
        textDT = topic[topic.rfind('_') + 1 :] + ',' + topic[: topic.rfind('_')]
        textDT = textDT.split(',')
        ab = text1.pop(1)
        str = ','.join(text1)
        text1 = [ab, '-', '-', '-', str]
        print(type(text1), ' - ', text1)

    # Villa_bme280_yama
    # Villa_bme280_base
    # text1 = [18.80,759,58,3.30]
    elif len(text1) == 4:
        textDT = topic[: topic.find('_')] + ',' + topic[topic.rfind('_', 2) + 1 :].capitalize()
        textDT = textDT.split(',')
        text1.append('-') 

    # Villa_am2320_house
    # text1 = [21.50,62,3.18]
    elif len(text1) == 3:
        textDT = topic[: topic.find('_')] + ',' +  topic[topic.rfind('_', 2) + 1 :].capitalize()
        textDT = textDT.split(',')
        text1.insert(1, '-')
        text1.append('-')

    else:
        print('FALSE !!!')
        return 1

    textDT += [time.strftime('%Y-%m-%d', time.localtime())] + [time.strftime('%H-%M-%S', time.localtime())]

    text3 = textDT + text1
    print(text3)

    query = """INSERT INTO sensors (loc, name, date, times, temp, pres, hum, vcc, other) VALUES (?,?,?,?,?,?,?,?,?);"""
    cursor.execute(query, text3)
    conn.commit()
    conn.close()
    print('Data write DB - OK !')
    return 0


def run():
    try:
        client = connect_mqtt()
        subscribe(client)
        client.loop_forever()
    except KeyboardInterrupt:
        global path1, folder_archive_name, my_file1
        if path1 != '':
            fileWrite(folder_archive_name, path1, my_file1)

        print("\nBye bye !")
        return 0


if __name__ == "__main__":
    run()
