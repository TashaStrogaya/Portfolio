
/*Собираю данные по когортам с информацией о удержанных, исчезнувших и новых пользователях на конец каждой недели*/

select 
  toString(t2.cohort_date) as cohort,                       -- имя когорты
  week_distance,                                            -- вовзращаемость (удержание) по количеству недель, 
  event_date + interval 6 day as end_of_week,               -- Дата конца недели
  -- users,                                                   -- количество пользователей для каждой кагорты каждого days_distance
  -- max_users,                                               -- количество пользователей в нулевой день
  max_users - users as churn_users,                         -- исчезнувшиеся пользователи
  if(event_date = cohort_date, max_users, 0) as new_users,  -- новые пользователи
  if(week_distance != 0, users, 0) as retained_users        -- удержанные пользователи

/*собираю удерживаемость пользователей по когортам*/
FROM 
(
  /*считаю по каждой кагорте и вовзращаемости количество пользователей*/
  select  
    birthday as cohort_date,
      week_distance,
      date_add(week, week_distance, birthday) as event_date,
      uniqExact(user_id)  as users
      
    from (
        /*разбиваю пользователей по когортам. Считаю возвращаемость относительно нулевого (birthday) дня*/
        select 
               feed.user_id,
               bd.birthday,
               (toStartOfWeek(feed.time, 1) - bd.birthday)/7 as week_distance
        from simulator_20220820.feed_actions feed 
        
        inner join (
              /*Отбираю день рождения в сервисе юзера*/
              SELECT 
                user_id, 
                min(toStartOfWeek(time, 1)) as birthday
              from simulator_20220820.feed_actions
              group by user_id
        ) bd
        
          on feed.user_id = bd.user_id
        
          /*отбираю только пользователей за последние две недели последнего календарного месяца*/
          /*
        where days_distance between 0 and 14
              and birthday between toStartOfMonth(now(), 1) - interval 15 day and toStartOfMonth(now(), 1)- interval 1 day*/
    ) t1
    group by birthday as cohort_date,
      week_distance
) t2

/*считаю для каждой кагорты объём пользователь в нулевой день*/
inner join  
(  
      select
        birthday as cohort_date,
        uniqExact(user_id)  as max_users
      
      from (
        /*разбиваю пользователей по когортам. Считаю возвращаемость относительно нулевого (birthday) дня*/
          select 
                 feed.user_id,
                 bd.birthday,
                 (toStartOfWeek(feed.time, 1) - bd.birthday)/7 as week_distance,
                 source
                 
          from simulator_20220820.feed_actions feed 
          inner join (
              /*Отбираю день рождения в сервисе юзера*/
                SELECT user_id, min(toStartOfWeek(time, 1)) as birthday
                from simulator_20220820.feed_actions
                group by user_id
          ) bd
          
            on feed.user_id = bd.user_id
          /*отбираю только пользователей за последние две недели последнего календарного месяца*/
          /*
          where days_distance between 0 and 14
                and birthday between toStartOfMonth(now(), 1) - interval 15 day and toStartOfMonth(now(), 1)- interval 1 day*/
      ) t0
      group by birthday as cohort_date
  ) tab
using cohort_date

order by event_date, week_distance
