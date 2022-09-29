#подключаемся к библиотекам
import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from io import StringIO
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import numpy as np

import telegram
import io

# функция для извлечения данных
def ch_get_df(query='Select 1', host='', db = '', user='', password=''):
    
    connection = {'host': host,
                      'database': db,
                      'user': user, 
                      'password': password
                     }

    df = ph.read_clickhouse(query, connection=connection)
    return df

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.safronova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 9, 11, 0, 0, 0) #,
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_catch_your_alerts():

    
    @task()
    def extract():
        # Забираем данные
        query = '''
        select 
            feed.time as time,
            time_event,
            dau_feed,
            likes,
            views,
            ctr,
            dau_messenger,
            messeges
        from (
            select
                toStartOfFifteenMinutes(time) as time,
                formatDateTime(time, '%m/%d %R') as time_event,        
                uniqExact(user_id) as dau_feed,
                countIf(action, action = 'like') as likes,
                countIf(action, action = 'view') as views,
                round(likes/views * 100 , 2) as ctr
            from
               db.feed_actions  
            WHERE 
                time >= today() - 1 
                and time < toStartOfFifteenMinutes(now())
            group by 
                toStartOfFifteenMinutes(time) as time
        ) feed
        
        join (
            select
                toStartOfFifteenMinutes(time) as time,    
                uniqExact(user_id) as dau_messenger,
                count() as messeges
            from
                db.message_actions  
            WHERE 
                time >= today() - 1 
                and time < toStartOfFifteenMinutes(now())
            group by 
                toStartOfFifteenMinutes(time) as time
        ) msg
        using time
        order by time
            '''

        df = ch_get_df(query)
        return df
    
    my_token =  os.environ.get("REPORT_BOT_TOKEN")
    bot = telegram.Bot(token=my_token)
    
    # функция проверки метрики на алерт
    def check_metric(df, metric, n = 5, a = 3):
    
        # Расчёт квантилей в скользящем окне
        df['q75'] = df[metric].shift(1).rolling(n, min_periods=1).quantile(0.75)
        df['q25'] = df[metric].shift(1).rolling(n, min_periods=1).quantile(0.25)
        
        # Расчёт МКР и границ метрики
        df['iqr'] = df['q75'] - df['q25']
        df['up'] = df['q75'] + a * df['iqr']
        df['low'] = df['q25'] - a * df['iqr']
        
        # Смягченеие границ метрики
        df['up'] = df.up.rolling(3, min_periods = 1).mean()
        df['low'] = df.low.rolling(3, min_periods = 1).mean()
        
        # Проверка выхода значения за границы
        if df[metric].iloc[-1] < df['low'].iloc[-1] or  df[metric].iloc[-1] > df['up'].iloc[-1]:
            return df, 1
        return df, 0
    
    # Функция отправки алертов
    @task
    def send_alert(df, chat_id = 770699869):

        metrics = ['dau_feed', 'likes', 'views', 'ctr','dau_messenger', 'messeges']
        
        # Проверяем в цикле нормальность метрик
        for metric in metrics:
            df, flag_alert = check_metric(df, metric)
            
            # Если метрика дала алерт, формируем и отправляем сообщение
            if flag_alert == 1:
                
                messege = f"Коллега, алёрт по метрике {metric}\n" + \
                        f"Текущее значение: {df[metric].iloc[-1]}. Отклонение от предыдущего: {(int(df[metric].iloc[-1]) - int(df[metric].iloc[-2]))/df[metric].iloc[-2]*100:.2f}%\n" + \
                        f"Расчёт ведётся с окном в 15 минут.\n" + \
                        f"Ссылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/1784/"
                bot.sendMessage(chat_id=chat_id, text=messege)
                print(messege)
                
                sns.set(rc={'figure.figsize':(16, 8)})
                
                ax = sns.lineplot(data = df, x = 'time_event', y = 'up', color = '#228b22')
                ax = sns.lineplot(data = df, x = 'time_event', y = 'low', color = '#228b22')
                ax = sns.lineplot(data = df, x = 'time_event', y = metric, color = '#1c1c1c', label = metric.upper())
                
                ax.fill_between(x = df.time_event, y1 = df.low, y2 = df.up, color = '#228b22', alpha = 0.2, label = 'Зона нормальности')

                # Оставляем метки на оси абсцис и сетку только для каждого 12 значения
                sticks_x = []
                for ind, item in enumerate(df.time_event.unique()):
                                if ind % 12 == 0:
                                    sticks_x.append(item)            
                ax.set_xticks(sticks_x)
                
                plt.title(f'Алёрт по метрике {metric.upper()}', size = 16)
                plt.legend(loc = 'lower right')
                plt.ylabel('')
                plt.xlabel('')

                plt.tight_layout()  # Уменьшаем рамку
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'alert.png'
                plt.close()

                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                print(f'ALERT with {metric}! Plots sent successfuly.')
    
    # Запуск таска
    send_alert(extract())
    
# Объявление дага
dag_catch_your_alerts = dag_catch_your_alerts()
