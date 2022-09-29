
/*Собираю данные по когортам с информацией для каждой из них об объёме пользователй в нулевой день*/

select 
  toString(t2.cohort_date) as cohort,  -- имя когорты
  days_distance,        -- вовзращаемость, от 0 до 14 дней
  users_ads,            -- количество пользователей пришедших из рекламы для каждой кагорты каждого days_distance
  users_organic,        -- количество пользователей пришедших органически для каждой кагорты каждого days_distance
  max_ads,              -- количество пользователей в нулевой день пришедших из рекламы каждой кагорты
  max_organic           -- количество пользователей в нулевой день пришедших из органически каждой кагорты

/*собираю возвращаемость по когортам*/
FROM 
(
  /*считаю по каждой кагорте и вовзращаемости (от нуля до 14 дней) количество пользователей относительно их source*/
  select  
    birthday as cohort_date,
      days_distance,
      countIf(distinct user_id, source = 'ads')  as users_ads,
      countIf(distinct user_id, source = 'organic')  as users_organic 
      
    from (
        /*разбиваю пользователей по когортам, сохраняя их source. Считаю возвращаемость относительно нулевого (birthday) дня*/
        select 
               feed.user_id,
               bd.birthday,
               toDate(feed.time) - bd.birthday as days_distance,
               source
        from simulator_20220820.feed_actions feed 
        
        inner join (
              /*Отбираю день рождения в сервисе юзера*/
              SELECT 
                user_id, 
                min(toDate(time)) as birthday
              from simulator_20220820.feed_actions
              group by user_id
        ) bd
        
          on feed.user_id = bd.user_id
          /*отбираю только пользователей за последние две недели последнего календарного месяца*/
        where days_distance between 0 and 14
              and birthday between toDate('2022-08-15') and toDate('2022-08-28')
    ) t1
    group by birthday as cohort_date,
      days_distance
) t2

/*считаю для каждой кагорты объём пользователь в нулевой день*/
inner join  
(  
      select
      birthday as cohort_date,
      countIf(distinct user_id, source = 'ads')  as max_ads,
      countIf(distinct user_id, source = 'organic')  as max_organic 
      
      from (
        /*разбиваю пользователей по когортам, сохраняя их source. Считаю возвращаемость относительно нулевого (birthday) дня*/
          select 
                 feed.user_id,
                 bd.birthday,
                 toDate(feed.time) - bd.birthday as days_distance,
                 source
                 
          from simulator_20220820.feed_actions feed 
          inner join (
              /*Отбираю день рождения в сервисе юзера*/
                SELECT user_id, min(toDate(time)) as birthday
                from simulator_20220820.feed_actions
                group by user_id
          ) bd
          
            on feed.user_id = bd.user_id
          /*отбираю только пользователей за последние две недели последнего календарного месяца*/
          where days_distance between 0 and 14
                and birthday between toDate('2022-08-15') and toDate('2022-08-28')
      ) t0
      group by birthday as cohort_date
  ) tab
using cohort_date
