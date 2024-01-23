# Автоматический бот определяющий резкие скачки показателей в течение дня,
# отправляет сообщение о падении/увелечении метрики, с указанием времени и % отклонения


import pandas as pd
import telegram 
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse
import io
from datetime import datetime, timedelta, date
from matplotlib.backends.backend_pdf import PdfPages
from telegram import InputFile
from airflow import DAG
from airflow.decorators import dag, task

# параметры для DAG 
default_args = {
    'owner': 'n-abramkin', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries' : 2, # Кол-во попыток выполнить DAG
    'retry_delay' : timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2023, 10, 26, 0, 0) # Дата начала выполнения DAG
}
schedule_interval = '*/15 * * * *'

# создание бота
my_token = '6676234776:AAEddY0hx3lrbtLvMBxU1t6ke6qIN3zVIYc'
bot = telegram.Bot(token=my_token)
chat_id = 271311667
#-938659451

# info = bot.getUpdates()
# print(info[-1])

# запрос в базу данных
def ch_get_df (query):
    connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator'}
    
    result = pandahouse.read_clickhouse(query, connection=connection)
    return result

# отправка сообщения            
def message(text):
    return bot.sendMessage(chat_id=chat_id, text=text, parse_mode='HTML')

# отправка графика   
def send_graph():
    # определение буфера
    plot_object = io.BytesIO()
    # сохранение графика
    plt.savefig(plot_object)
    # перевод курсора
    plot_object.seek(0)
    plt.close()
    # отправка сообщения
    return bot.sendPhoto(chat_id=chat_id, photo=plot_object)

# создание графика
def graph(df, x, y, title):
    data = df[df['date'] == df['date'].iloc[-1]].reset_index(drop=True)
    # размер графика и разрешение
    plt.figure(figsize=(12, 4), dpi=150)
    # график линии
    sns.lineplot(data=data, x=x, y=y)
    # добавление точек на график
    sns.scatterplot(data=data, x=x, y=y, color='blue', s=10)
    # добавление чисел на график
    for i in range(0, len(data[x]), 6):
        plt.text(data[x][i], data[y][i], str(round(data[y][i], 3)), ha='center', va='bottom')
    # начало графика с 0
    plt.ylim(0, max(data[y])+(max(data[y])*0.25))
    value = data['hm'][::4]
    plt.xticks(value,rotation=45)
    # заголовок
    plt.title(title);
    send_graph()
    #pdf_pages.savefig(plt.gcf())
    return


@dag(default_args=default_args, schedule_interval=schedule_interval)
def n_abramkin_alert():
    
    @task()
    def alert():
        query = """
                SELECT toStartOfFifteenMinutes(time) AS ts,
                toDate(ts) AS date,
                formatDateTime(ts, '%R') AS hm,
                COUNT (DISTINCT user_id) AS DAU_feed,
                COUNTIf(user_id, action = 'view') AS count_views,
                COUNTIf (user_id, action = 'like') AS count_likes,
                ROUND(count_likes / count_views, 3) AS CTR,
                DAU_message, 
                message_sent
                FROM simulator_20230920.feed_actions AS fa
                JOIN 
                    (SELECT toStartOfFifteenMinutes(time) AS ts,
                    toDate(ts) AS date,
                    formatDateTime(ts, '%R') AS hm,
                    COUNT (DISTINCT user_id) AS DAU_message,
                    COUNT (user_id) AS message_sent
                    FROM simulator_20230920.message_actions AS ma
                    WHERE date BETWEEN today() -1 AND today()
                    GROUP BY ts, date, hm
                    ORDER BY ts) AS ma
                USING ts, date, hm

                WHERE date BETWEEN today() -1 AND today() AND ts < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm, DAU_message, message_sent
                ORDER BY ts
            """
        df = ch_get_df(query)
        #df = pd.read_csv('test.csv')

        # расчет % отклонения
        def percent(column):
            begin_value = float(df[column].iloc[-1])
            end_value =  float(df[column].iloc[-2])
            percent = round(((begin_value - end_value) / end_value) * 100, 2)
            out =  f'+{percent} %' if percent > 0 else f'{percent} %'
            return str(out)
        
        # нахождение доврительного интервала и проверка метрик
        def range_alert(column):
            Q1, Q2, Q3 = np.round(np.quantile(df[column].iloc[-5:-1], 
                                              q=[0.25, 0.5, 0.75], 
                                              interpolation='midpoint'), 3)
            # межквратильный размах
            IQR = round(Q3 - Q1, 3)
            # нижняя грaница
            upper_limit = round(Q1 - 1.5 * IQR, 3)
            # верхня граница (сделаем ее чуть больше, чтобы получать меньше
            # положительных алертов)
            bottom_limit = round(Q3 + 1.5 * IQR, 3)

            # проверка вчершнего значения метрики с сегодняшним
            current_ts = df['ts'].max() # максимальная 15 минутка
            day_ago_ts = current_ts - pd.DateOffset(days=1) # такая же 15 минутка 1 день назад
            today_value = int(df[df['ts'] == current_ts][column].iloc[0]) # значение сегодняшней 15 минутки
            day_ago_value = int(df[df['ts'] == day_ago_ts][column].iloc[0]) #  значение вчерашней 15 минутки

            # условие, чтобы исключить делениен на 0
            if today_value - day_ago_value == 0:
                percent_value = 0
            else:
                percent_value = round(((today_value - day_ago_value) 
                                       / day_ago_value) * 100, 2)

            # проверка: на сколько процентов поменялось метрика 
            # от медианы последнего часа. Чтобы улавливать сильные изменения 
            # а не волатильность в течение дня, добавим в условие проверку:
            # если метрика отличается более чем на 8 % и находится за пределами 
            # допустимого диапозона, то алерт сработает
            percent = round(((df[column].iloc[-1] - Q2) / Q2) * 100, 2)

            # положительное выбивающееся значение
            if df[column].iloc[-1] > bottom_limit \
                and (percent > 8 or percent_value > 5):
                alert = 1 
            # отрицательное выбивающееся значение
            elif df[column].iloc[-1] < upper_limit \
                and (percent < -8 or percent_value < -5):
                alert = -1 
            # нет выбивающихся значений
            else:
                alert = 0 
            return alert
        
        # проверка метрики на аномальность и ее вывод
        for i in df.columns.drop(['ts', 'date', 'hm']):
            alert = range_alert(i)
            if alert in [-1, 1]:
                msg = f'''<b>Обнаружено аномальное значение!</b>
<b>Метрика: </b> {i};
<b>Значение на {df['hm'].iloc[-1]}:</b> {str(df[i].iloc[-1])};
<b>Значение на {df['hm'].iloc[-2]}:</b> {str(df[i].iloc[-2])};
<b>Отклонение на </b> {percent(i)} или {str(int(df[i].iloc[-1]) - int(df[i].iloc[-2]))}.'''
                message(msg)
                graph(df, 'hm', i, i)
    
        return

    alert()
    
n_abramkin_alert = n_abramkin_alert()
