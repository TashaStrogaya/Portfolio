from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pandahouse as ph
import numpy as np

def ch_get_df(query='Select 1', host='', user='', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.safronova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2022, 9, 6) #,
}

# Интервал запуска DAG
schedule_interval = '@daily'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_get_groups_agg_nsaf():

    # извлекаем пользователей ленты
    @task()
    def extract_feed():
        query = """
            SELECT *
            FROM 
                db.feed_actions 
            where 
                toDate(time) = toDate(now() - interval 1 day)
            format TSVWithNames"""
        df_cube_feed = ch_get_df(query=query)
        return df_cube_feed

    # извлекаем пользователей сервиса сообщений
    @task()
    def extract_mesg():
        query = """
            SELECT *
            FROM 
                db.message_actions 
            where 
                toDate(time) = toDate(now() - interval 1 day)
            format TSVWithNames"""
        df_cube_mesg = ch_get_df(query=query)
        return df_cube_mesg

    # собираем агрегированную информацию по юзерам ленты
    @task
    def transfrom_feed(df_feed):
        df_feed['gender_label'] = np.where(df_feed.gender == 1,'male', 'female')

        df_feed_views = df_feed.groupby('user_id')['action'].apply(lambda x: (x=='view').sum()).reset_index(name='views')
        df_feed_likes = df_feed.groupby('user_id')['action'].apply(lambda x: (x=='like').sum()).reset_index(name='likes')

        df_feed_user = df_feed.groupby('user_id', as_index = False).agg(os = ('os', 'max'), gender = ('gender_label', 'max'), age = ( 'age', 'max'))

        df_feed_agg = df_feed_likes.merge(df_feed_views)
        df_feed_all = df_feed_user.merge(df_feed_agg)

        return df_feed_all

    # собираем агрегированную информацию по юзерам сообщений
    @task
    def transfrom_mesg(df_mesg):
        df_mesg['gender_label'] = np.where(df_mesg.gender == 1,'male', 'female')

        df_mesg_sent = df_mesg.groupby('user_id', as_index = False).agg(messages_sent = ('reciever_id', 'count'), users_sent = ('reciever_id', 'nunique'), os = ('os', 'max'), 
                                                                        gender = ('gender_label', 'max'), age = ( 'age', 'max'))
        df_mesg_received = df_mesg.groupby('reciever_id', as_index = False).agg(messages_received = ('user_id', 'count'), users_received = ('user_id', 'nunique'))
        df_mesg_received.rename(columns={'reciever_id': 'user_id'}, inplace=True)

        df_mesg_all = df_mesg_received.merge(df_mesg_sent, how='outer')
        df_mesg_all.fillna(0, inplace=True)

        return df_mesg_all
    
    # объёдиняем агрегрированную информацию
    @task
    def common_tabs(tab1, tab2):
        users = tab1.merge(tab2, how = 'outer', on = 'user_id', suffixes=('', '_right'))
        users.fillna(0, inplace=True)

        users['os'] = np.where(users.os == 0, users.os_right, users.os)
        users['age'] = np.where(users.age == 0, users.age_right, users.age)
        users['gender'] = np.where(users.gender == 0, users.gender_right, users.gender)
        users = users.drop(['os_right', 'gender_right', 'age_right'], axis = 1)

        users['os'] = np.where(users.os == 0, 'unknown', users.os)
        users['age'] = np.where(users.age == 0, 'unknown', users.age)
        users['gender'] = np.where(users.gender == 0, 'unknown', users.gender)

        return users
    
    # создаем срез пол ОС
    @task
    def get_os(df):
        df_tmp = df.drop(['gender', 'age', 'user_id'], axis=1)
        df_os = df_tmp.groupby('os', as_index = False).agg('sum')

        df_os.rename(columns={'os': 'dimension_value'}, inplace=True)
        df_os['dimension'] = 'os'
        df_os['event_date'] = datetime.date(datetime.today() - timedelta(days=1))

        df_os = df_os[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]

        return df_os
    
    # создаём срез по полу
    @task     
    def get_gender(df):
        df_tmp = df.drop(['os', 'age', 'user_id'], axis=1)
        df_gender = df_tmp.groupby('gender', as_index = False).agg('sum')

        df_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)
        df_gender['dimension'] = 'gender'
        df_gender['event_date'] = datetime.date(datetime.today() - timedelta(days=1))

        df_gender = df_gender[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]

        return df_gender
    
    # создаем срез по возрасту
    @task
    def get_age(df):
        df_tmp = df.drop(['os', 'gender', 'user_id'], axis=1)
        df_age = df_tmp.groupby('age', as_index = False).agg('sum')

        df_age.rename(columns={'age': 'dimension_value'}, inplace=True)
        df_age['dimension'] = 'age'
        df_age['event_date'] = datetime.date(datetime.today() - timedelta(days=1))

        df_age = df_age[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]

        return df_age
    
    # объединяем агрегированную по срезам информацию
    @task
    def get_all_groups_metrics(df_os, df_gender, df_age):
        df_goal = pd.concat([df_os, df_gender, df_age], ignore_index = True)
        return df_goal
    
    # выгрузка таблицы
    @task
    def load(df_final):
        load_connection = {}

        print('Data preparation for sending...')

        query_cr = '''CREATE TABLE IF NOT EXISTS test.groups_activity_nsaf 
                            (
                            event_date DATE, 
                            dimension TEXT, 
                            dimension_value TEXT, 
                            views UInt64, 
                            likes UInt64, 
                            messages_received UInt64, 
                            messages_sent UInt64, 
                            users_received UInt64, 
                            users_sent UInt64
                            )
                            ENGINE = Log()'''
        ph.execute(connection=load_connection, query=query_cr)

        df_final.views = df_final.views.astype('int64')
        df_final.likes = df_final.likes.astype('int64')
        df_final.messages_received = df_final.messages_received.astype('int64')
        df_final.messages_sent = df_final.messages_sent.astype('int64')
        df_final.users_received = df_final.users_received.astype('int64')
        df_final.users_sent = df_final.users_sent.astype('int64')


        ph.to_clickhouse(df_final, 'groups_activity_nsaf', connection=load_connection, index=False)
        print('Database updated.')

        query_final = """
            SELECT * FROM 
            test.groups_activity_nsaf
            where event_date = today() - interval 1 day
        """

        df_download = ph.read_clickhouse(query_final, connection=load_connection)
        context = get_current_context()
        ds = context['ds']
        print(f'Users groups data for {ds}')
        # print(f'\nUsers groups data for {(datetime.today() - timedelta(days=1)).date()}')
        print('\n')
        print(df_download)
        print('\n The table |test.groups_activity_nsaf| successfuly created.')


    df_feed = extract_feed()
    df_mesg = extract_mesg()
    
    df_feed_agg = transfrom_feed(df_feed)
    df_mesg_agg = transfrom_mesg(df_mesg)
    users = common_tabs(df_feed_agg, df_mesg_agg)
    
    df_os = get_os(users)
    df_age = get_age(users)
    df_gender = get_gender(users)
    
    df_goal = get_all_groups_metrics(df_os, df_gender, df_age)
    
    load(df_goal)

# cостаются поля 'unknown', от которых можно избавить добиранием информации и
# дополнительной таблицы с информацией только по пользователям приложения
# на этапе таблицы users по юзер_айди насытить поля gender, os, age.
# Пустые поля появляются, тк есть пользователи, которым только написали, сами же они не проявляли активности
# ни в ленте, ни в сообщениях.

#запускаем даг
dag_get_groups_agg_nsaf = dag_get_groups_agg_nsaf()
