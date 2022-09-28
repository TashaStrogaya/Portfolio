#подключаемся к библиотекам
import pandas as pd
import pandahouse as ph
from scipy import stats
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

# Функция для забора данных
def ch_get_df(query='Select 1', host='', user='', password=''):

    connection = {}

    df = ph.read_clickhouse(query, connection=connection)
    return df

# Дефолтные параметры, которые прокидываются в таски
default_args = {
'owner': 'n.safronova',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=15),
'start_date': datetime(2022, 9, 12, 0, 0, 0) #,
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_friendly_full_report_bot():

    # Подключение к боту
    my_token = os.environ.get("REPORT_BOT_TOKEN")
    bot = telegram.Bot(token=my_token)

    @task
    # Забираем данные активностей ленты, агрегированных по дням
    def extract_feed():
        query = '''
        select 
            toDate(time) as event_date,                                                     -- День сводки
            uniqExact(user_id) as DAU,                                                      -- DAU
            countIf(distinct user_id, birthday = event_date) as new_users,                  -- Новые пользователи
            countIf(distinct user_id, birthday + interval 1 day = event_date) as ret_1day,  -- Вернувшиеся спустя один день
            countIf(distinct user_id, country != 'Russia') as foreign_users,                -- Зарубежные пользователи
            
            countIf(action, action = 'view') as views,                                      -- Просмотры
            round(views/DAU, 2) as views_per_users,                                         -- Просмотры на пользователя
            countIf(action, action = 'like') as likes,                                      -- Лайки
            round(likes/DAU, 2) as likes_per_users,                                         -- Лайки на пользователя
            
            round(likes/views*100, 2) as CTR,                                               -- CTR
            uniqExact(post_id) as active_posts,                                             -- Активные посты
            
            countIf(distinct user_id, source = 'ads') as ads,                               -- Рекламные пользователи
            countIf(distinct user_id, source = 'organic') as organic,                       -- Органические пользователи
            round(ads/DAU * 100, 2) as ads_per_dau,                                         -- Доля рекламных пользователей
            round(organic/DAU * 100, 2) as organic_per_dau,                                 -- Доля органических пользователей
            
            countIf(distinct user_id, os = 'Android') as android,                           -- Android пользователи
            countIf(distinct user_id, os = 'iOS') as ios,                                   -- iOS пользователи
            round(android/DAU * 100, 2) as android_per_dau,                                 -- Доля Android пользователей
            round(ios/DAU * 100, 2) as ios_per_dau                                          -- Доля iOS пользователей
            

        from
            db.feed_actions 

        -- Извлекаем день рождения пользователя, чтобы построить метрику кол-ва новых пользовтелей и ретеншена на 1 день
        join (
            select 
            min(toDate(time)) as birthday,
            user_id
            FROM 
            db.feed_actions 
            group BY 
            user_id
        ) bd
        using user_id

        -- Отбираю данные только за последний месяц, для расчёта Stickness, сами графики будут затрагивать окно в последние две недели
        WHERE 
            toDate(time) between today() - interval 30 day and today() - interval 1 day
        group by 
            toDate(time) as event_date
        '''

        df = ch_get_df(query)
        df['ret_1day_ratio'] = round(df.ret_1day/df.new_users.shift(1, fill_value = 0) * 100, 2)
        return df

    @task
    def extract_mesg():
        # Забираем данные
        query = '''
        select
            toDate(time) as event_date,                                                     -- День сводки
            uniqExact(user_id) as users_received,                                           -- DAU
            countIf(distinct user_id, birthday = event_date) as new_users,                  -- Новые пользователи
            countIf(distinct user_id, birthday + interval 1 day = event_date) as ret_1day,  -- Вовзвращаемость спустя 1 день
            
            count() as messeges,                                                            -- Количество сообщений
            round(messeges/users_received, 2) as messeges_per_user,                         -- Сообщений на пользователя
            uniqExact(reciever_id) as users_sent,                                           -- Количество получетелй писем
            round(users_received/users_sent, 2) as recieved_user_per_sent,                  -- Отношенеи пишущих к получающим
            
            countIf(distinct user_id, source = 'ads') as ads_users,                         -- Рекламные пользователи
            countIf(distinct user_id, source = 'organic') as organic_users,                 -- Органические пользователи
            round(ads_users/users_received * 100, 2) as ads_per_dau,                        -- Доля рекламных пользователей
            round(organic_users/users_received * 100, 2) as organic_per_dau,                -- Доля органических пользователей
            
            countIf(distinct user_id, os = 'Android') as android,                           -- Android пользователи
            countIf(distinct user_id, os = 'iOS') as ios,                                   -- iOS пользователи
            round(android/users_received * 100, 2) as android_per_dau,                      -- Доля Android пользователей
            round(ios/users_received * 100, 2) as ios_per_dau                               -- Доля iOS пользователей
        from
            db.message_actions

        -- Извлекаем день рождения пользователя, чтобы построить метрику кол-ва новых пользовтелей и ретеншена на 1 день
        join (
            select 
            min(toDate(time)) as birthday,
            user_id
            FROM 
            db.message_actions 
            group BY 
            user_id
        ) bd
        using user_id

        -- Отбираю данные только за последний месяц, для расчёта Stickness, сами графики будут затрагивать окно в последние две недели
        WHERE 
            toDate(time) between today() - interval 30 day and today() - interval 1 day
        group by 
            toDate(time) as event_date
        '''

        df = ch_get_df(query)
        df['ret_1day_ratio'] = round(df.ret_1day/df.new_users.shift(1, fill_value = 0) * 100, 2)
        return df

    # Забор списка пользователей, которые пользовались нашими сервисами по дням
    @task
    def extract_common():
        # Забираем данные по ленте
        query = '''
        select
            user_id,
            toDate(time) as event_date,
            'feed' as service
        from
            db.feed_actions
        where
            toDate(time) between today() - interval 14 day and today() - interval 1 day
        group by
            toDate(time) as event_date,
            user_id

        union all

        -- Забираем данные по мессенджеру
        select
            user_id,
            toDate(time) as event_date,
            'message' as service
        from
            db.message_actions
        where
            toDate(time) between today() - interval 14 day and today() - interval 1 day
        group by
            toDate(time) as event_date,
            user_id
        '''

        df = ch_get_df(query)
        return df

    # Функция перевода агрегированной информации о ленте в сообщение по вчерашнему дню + WoW (относительная разница с метрикой неделю назад)
    @task
    def get_full_yesterday_feed_activity(df):  

        yesterday = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(1)

        # Получение словаря со значениями метрик вчера
        yest = df[df.event_date == yesterday].to_dict('records')[0]
        # И получения словаря со значениями метрик неделю назад
        prev = df[df.event_date == yesterday - timedelta(7)].to_dict('records')[0]

        # Расчёт WoW исследуемых метрик
        lifts = {}
    
        metrics = ['DAU', 'new_users', 'ret_1day_ratio', 'foreign_users', 'likes_per_users', 'views_per_users',  \
                'CTR', 'active_posts', 'ads_per_dau', 'organic_per_dau', 'android_per_dau', 'ios_per_dau']
        
        for metric in metrics:
            lifts[metric] = (yest[metric] - prev[metric]) / prev[metric] * 100

        MAU = df.DAU.sum()
        
        # Текстовый отчет о работе ленты
        msg_agg =  f"Привет, Коллега!\nЭто твой отчёт по приложению за {yesterday.date()} | + WoW %\n\nПОЛЬЗОВАТЕЛЬСКАЯ АКТИВНОСТЬ В ЛЕНТЕ:" + \
        f"\n\nDAU: {yest['DAU']:_} | {lifts['DAU']:.2f}%\nStickness: {yest['DAU']/MAU*100:.2f}%\n" + \
        f"Новые пользователи: {yest['new_users']:_} | {lifts['new_users']:.2f}%\nВозвращаемость на 1 день: {yest['ret_1day_ratio']}% | {lifts['ret_1day_ratio']:.2f}%\n" + \
        f"Зарубежные пользователи: {yest['foreign_users']:_} | {lifts['foreign_users']:.2f}%\n\n" + \
        f"Лайков per user: {yest['likes_per_users']:_} | {lifts['likes_per_users']:.2f}%\nПросмотров per user: {yest['views_per_users']:_} | {lifts['views_per_users']:.2f}%\n" + \
        f"Глобальный CTR: {yest['CTR']}% | {lifts['CTR']:.2f}%\nАктивные посты: {yest['active_posts']} | {lifts['active_posts']:.2f}%\n" + \
        f"\nРекламные пользователи: {yest['ads_per_dau']}% | {lifts['ads_per_dau']:.2f}%\nОрганик пользователи: {yest['organic_per_dau']}% | {lifts['organic_per_dau']:.2f}%\n" + \
        f"Andriod пользователи: {yest['android_per_dau']}% | {lifts['android_per_dau']:.2f}%\niOS пользователи: {yest['ios_per_dau']}% | {lifts['ios_per_dau']:.2f}%\n"

        return msg_agg

    # Функция перевода агрегированной информации о мессенджере в сообщение по вчерашнему дню + WoW (относительная разница с метрикой неделю назад)
    @task
    def get_full_yesterday_mesg_activity(df):

        yesterday = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(1)

        # Получение словаря со значениями метрик вчера
        yest = df[df.event_date == yesterday].to_dict('records')[0]
        # И получения словаря со значениями метрик неделю назад
        prev = df[df.event_date == yesterday - timedelta(7)].to_dict('records')[0]

        # Расчёт WoW исследуемых метрик
        lifts = {}
    
        metrics = ['users_received', 'new_users', 'ret_1day_ratio', 'users_sent', 'recieved_user_per_sent', 'messeges', 'messeges_per_user', \
                'ads_per_dau', 'organic_per_dau', 'android_per_dau', 'ios_per_dau']
        
        for metric in metrics:
            lifts[metric] = (yest[metric] - prev[metric]) / prev[metric] * 100
        
        MAU = df.users_received.sum()

        # Сбор единого сообщения с отчётом по мессенджеру в целом

        msg_agg =  f"\nПОЛЬЗОВАТЕЛЬСКАЯ АКТИВНОСТЬ В СЕРВИСЕ СООБЩЕНИЙ\nМетрики расcчитываются относительно пользователей, отправивших сообщение:" + \
        f"\n\nDAU: {yest['users_received']:_} | {lifts['users_received']:.2f}%\nStickness: {yest['users_received']/MAU*100:.2f}%" + \
        f"\nНовые пользователи: {yest['new_users']:_} | {lifts['new_users']:.2f}%\nВозвращаемость на 1 день: {yest['ret_1day_ratio']}% | {lifts['ret_1day_ratio']:.2f}%\n\n" + \
        f"Количество сообщений: {yest['messeges']:_} | {lifts['messeges']:.2f}%\nСообщений per user: {yest['messeges_per_user']} | {lifts['messeges_per_user']:.2f}%" + \
        f"\nПолучатели сообщений: {yest['users_sent']:_} | {lifts['users_sent']:.2f}%\n" + \
        f"Отношение пишущих к получившим сообщение: {yest['recieved_user_per_sent']:} | {lifts['recieved_user_per_sent']:.2f}%\n" + \
        f"\nРекламные пользователи: {yest['ads_per_dau']}% | {lifts['ads_per_dau']:.2f}%\nОрганик пользователи: {yest['organic_per_dau']}% | {lifts['organic_per_dau']:.2f}%\n" + \
        f"Andriod пользователи: {yest['android_per_dau']}% | {lifts['android_per_dau']:.2f}%\niOS пользователи: {yest['ios_per_dau']}% | {lifts['ios_per_dau']:.2f}%\n\n"


        return msg_agg

    # Считаю метрики актуальности каждого сервиса/в целом для пользователей наших сервисов
    @task
    def get_full_yesterday_common_activity(df): 

        yesterday = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(1)
        
        yest =  df[df.event_date == yesterday].copy()
        prev = df[df.event_date == yesterday - timedelta(7)].copy()

        # Считаю метрики актуальности сервисов за вчера и со смещением в неделю назад
        metrics = ['feed_users', 'mesg_users', 'full_users']
    
        res = {metric: [] for metric in metrics}
        
        for item in [yest, prev]:
            lst_feed = item.query('service == "feed"')['user_id'].to_numpy()
            lst_mesg = item.query('service == "message"')['user_id'].to_numpy()
            
            res['feed_users'].append(sum(i not in lst_mesg for i in lst_feed))
            res['mesg_users'].append(sum(i not in lst_feed for i in lst_mesg))
            res['full_users'].append(sum(i in lst_feed for i in lst_mesg))

        # Считаю WoW
        lift = {}
        for metric in metrics:
            lift[metric] = (res[metric][0] - res[metric][1]) / res[metric][1] * 100

        # Собираю итоговое сообщение
        msg_agg = \
        f"ПОПУЛЯРНОСТЬ СЕРВИСОВ:\n\n" + \
        f"Пользовтели только ленты: {res['feed_users'][0]:_} | {lift['feed_users']:.2f}%\n" + \
        f"Пользователи только сервиса сообщений: {res['mesg_users'][0]:_} | {lift['mesg_users']:.2f}%\n" + \
        f"Пользователи обоих сервисов: {res['full_users'][0]:_} | {lift['full_users']:.2f}%\n"

        return msg_agg

    # Функция отправки боту отчёта, по умолчанию отправляет отчёт автору бота
    # Функция построения графиков и отправки их совместно с текстовым отчетом
    @task
    def gets_plots_and_sending_all(df_feed, df_mesg, feed_txt, mesg_txt, common_txt, chat_id = 770699869):

        # Отправление текстового отчета
        messege = feed_txt + mesg_txt + common_txt
        bot.sendMessage(chat_id=chat_id, text=messege)
        print('Text was sent.')

        yesterday = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(1)

        # Преобразование датафрейма ленты в два датафрейма, содержащих информацию за последнюю неделю и за предпоследнюю
        feed_week = df_feed[df_feed.event_date > yesterday - timedelta(7)].copy()
        feed_week['short_date'] = feed_week["event_date"].dt.strftime('%b %d')

        feed_week_prev = df_feed[df_feed.event_date <= yesterday - timedelta(7)][df_feed.event_date > yesterday - timedelta(14)].copy()
        feed_week_prev['short_date'] = (feed_week_prev["event_date"] + timedelta(7)).dt.strftime('%b %d')

        ##################################################################################################################################

        # Создание списков заголовоков и метрик для графиков
        titles_feed1 = ['DAU', 'Новые пользователи', 'Возвращаемость на 1 день, %', 'Лайки на пользователя', 'Просмотры на пользователя', 'Глобальный CTR, %']

        metrics_feed1 = ['DAU', 'new_users', 'ret_1day_ratio', 'likes_per_users', 'views_per_users', 'CTR']


        feed_p1 = {metric : title for metric, title in zip(metrics_feed1, titles_feed1)}

        # Построение всех графиков, описывающих поведение ленты за последние две недели
        sns.set(rc={'figure.figsize':(20,10)})
        plt.suptitle('Данные ЛЕНТЫ за последнюю неделю и предпоследнюю(--) часть 1', size = 16, fontstyle = 'italic')

        i = 1
        for metric, title in feed_p1.items():
            plt.subplot(2, 3, i)
            sns.lineplot(data = feed_week, x = 'short_date', y = metric,  marker='o', color = '#8b008b')
            sns.lineplot(data = feed_week_prev, x = 'short_date', y = metric,  marker='o', color = '#ed3cca', linestyle = '--')
            plt.title(title, size = 15)
            plt.ylabel('')
            plt.xlabel('')
            i += 1

        # Подготовка и отправка боту визуального отчета по ленте
        plt.tight_layout()
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot_feed1.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        titles_feed2 = ['Зарубежные пользователи', 'Доля рекламных пользователей, %', 'Доля органических пользователей, %', \
                'Активные посты', 'Доля Android пользователей, %', 'Доля iOS пользователей, %']

        metrics_feed2 = ['foreign_users', 'ads_per_dau', 'organic_per_dau', 'active_posts', 'android_per_dau', 'ios_per_dau']


        feed_p2 = {metric : title for metric, title in zip(metrics_feed2, titles_feed2)}

        # Построение всех графиков, описывающих поведение ленты за последние две недели
        sns.set(rc={'figure.figsize':(20,10)})
        plt.suptitle('Данные ЛЕНТЫ за последнюю неделю и предпоследнюю(--) часть 2', size = 16, fontstyle = 'italic')

        i = 1
        for metric, title in feed_p2.items():
            plt.subplot(2, 3, i)
            sns.lineplot(data = feed_week, x = 'short_date', y = metric,  marker='o', color = '#8b008b')
            sns.lineplot(data = feed_week_prev, x = 'short_date', y = metric,  marker='o', color = '#ed3cca', linestyle = '--')
            plt.title(title, size = 15)
            plt.ylabel('')
            plt.xlabel('')
            i += 1

        # Подготовка и отправка боту визуального отчета по ленте
        plt.tight_layout()
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot_feed2.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        ##################################################################################################################################

        # Преобразование датафрейма ленты в два датафрейма, содержащих информацию за последнюю неделю и за предпоследнюю    
        mesg_week = df_mesg[df_mesg.event_date > yesterday - timedelta(7)].copy()
        mesg_week['short_date'] = mesg_week["event_date"].dt.strftime('%b %d')
        mesg_week['messeges'] = round(mesg_week['messeges'] / 1000, 2)  # Преобразование значения колонки для более удобного восприятия

        mesg_week_prev = df_mesg[df_mesg.event_date <= yesterday - timedelta(7)][df_mesg.event_date > yesterday - timedelta(14)].copy()
        mesg_week_prev['short_date'] = (mesg_week_prev["event_date"] + timedelta(7)).dt.strftime('%b %d')
        mesg_week_prev['messeges'] = round(mesg_week_prev['messeges'] / 1000, 2)

        # Создание списков заголовоков и метрик для графиков
        titles = ['DAU', 'Новые пользователи', 'Возвращаемость на 1 день, %', 'Сообщения в тысячах', \
                'Доля рекламных пользователей, %', 'Доля органических пользователей, %', \
                'Сообщений на пользователя', 'Доля Android пользователей, %', 'Доля iOS пользователей, %']
        
        metrics_feed = ['users_received', 'new_users', 'ret_1day_ratio', 'messeges', \
                'ads_per_dau', 'organic_per_dau', 'messeges_per_user', 'android_per_dau', 'ios_per_dau']

        
        mesges = {metric : title for metric, title in zip(metrics_feed, titles)}
        
        # Построение всех графиков, описывающих поведение ленты за последние две недели
        sns.set(rc={'figure.figsize':(21,14)})
        plt.suptitle('Данные МЕССЕНДЖЕРА за последнюю неделю и предпоследнюю(--)', size = 18, fontstyle = 'italic')
        
        i = 0
        for metric, title in mesges.items():
            plt.subplot(3, 3, i+1)
            sns.lineplot(data = mesg_week, x = 'short_date', y = metric,  marker='o', color = '#8b008b')
            sns.lineplot(data = mesg_week_prev, x = 'short_date', y = metric,  marker='o', color = '#ed3cca', linestyle = '--')
            plt.title(title, size = 15)
            plt.ylabel('')
            plt.xlabel('')
            i += 1
        
        # Подготовка и отправка боту визуального отчета по ленте
        plt.tight_layout()
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot_mesg.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        print('Plots successfuly sent.')

    # Запуск тасок эирфлоу
    df_common = extract_common()
    df_mesg = extract_mesg()
    df_feed = extract_feed()

    feed_txt = get_full_yesterday_feed_activity(df_feed)
    mesg_txt = get_full_yesterday_mesg_activity(df_mesg)
    common_txt = get_full_yesterday_common_activity(df_common)

    gets_plots_and_sending_all(df_feed, df_mesg, feed_txt, mesg_txt, common_txt)

# Запуск дага
dag_friendly_full_report_bot = dag_friendly_full_report_bot()
