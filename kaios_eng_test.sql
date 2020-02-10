
select app_id, app_views, app_installs , round(app_installs/app_views , 4) as app_conversion_rate from (
select app_id, sum(view_count) as app_views, sum(new_install_count)+sum(old_install_count) as app_installs from (
select app_id ,  user_id , sum(case when event_type='store_app_view' then 1 else 0 end) as view_count, sum(case when event_type='store_app_install' then 1 else 0 end) as new_install_count, 
sum(case when event_type='store_app_view' and next_event_type ='store_app_download' then 1 else 0 end) as old_install_count   from

(select app_id , user_id ,  event_time_utc ,event_type,Lead(event_type, 1)  OVER (PARTITION BY app_id , user_id ORDER BY event_time_utc ) as next_event_type
from store_events
) as se
group by app_id, user_id) as se2
group by app_id) as se3


/*
Target fields:
app_views
app_installs
app_conversion_rate
store_open


Events :
store_app_view
store_app_download
store_app_install
store_app_update
store_fetch_manifest

Test data patch:

insert into store_events values ('u1','app1','store_app_view','2020-01-01 19:10:25');
insert into store_events values ('u1','app1','store_app_view','2020-01-01 19:10:26');
insert into store_events values ('u1','app1','store_app_install','2020-01-01 19:10:27');
insert into store_events values ('u1','app1','store_app_view','2020-01-01 19:10:30');
insert into store_events values ('u1','app1','store_app_update','2020-01-01 19:10:31');
insert into store_events values ('u1','app1','store_fetch_manifest','2020-01-01 19:10:35');
insert into store_events values ('u1','app1','store_app_view','2020-01-01 19:10:39');
insert into store_events values ('u1','app1','store_app_install','2020-01-01 19:10:40');
insert into store_events values ('u1','app1','store_app_download','2020-01-01 19:10:42');
insert into store_events values ('u1','app1','store_app_view','2020-01-01 19:10:43');
insert into store_events values ('u1','app1','store_app_download','2020-01-01 19:10:44');


insert into store_events values ('u2','app1','store_app_view','2020-01-01 19:10:25');
insert into store_events values ('u2','app1','store_app_view','2020-01-01 19:10:26');
insert into store_events values ('u2','app1','store_app_install','2020-01-01 19:10:27');
insert into store_events values ('u2','app1','store_app_view','2020-01-01 19:10:30');
insert into store_events values ('u2','app1','store_app_update','2020-01-01 19:10:31');
insert into store_events values ('u2','app1','store_fetch_manifest','2020-01-01 19:10:35');
insert into store_events values ('u2','app1','store_app_view','2020-01-01 19:10:39');
insert into store_events values ('u2','app1','store_app_install','2020-01-01 19:10:40');
insert into store_events values ('u2','app1','store_app_download','2020-01-01 19:10:42');
insert into store_events values ('u2','app1','store_app_view','2020-01-01 19:10:43');
insert into store_events values ('u2','app1','store_app_download','2020-01-01 19:10:44');


insert into store_events values ('u1','app2','store_app_view','2020-01-02 19:10:25');
insert into store_events values ('u1','app2','store_app_download','2020-01-02 19:10:26');
insert into store_events values ('u1','app2','store_app_install','2020-01-02 19:10:27');
insert into store_events values ('u1','app2','store_app_view','2020-01-02 19:10:30');
insert into store_events values ('u1','app2','store_app_update','2020-01-02 19:10:31');
insert into store_events values ('u1','app2','store_fetch_manifest','2020-01-02 19:10:35');
insert into store_events values ('u1','app2','store_app_view','2020-01-02 19:10:39');
insert into store_events values ('u1','app2','store_app_install','2020-01-02 19:10:40');
insert into store_events values ('u1','app2','store_app_download','2020-01-02 19:10:42');
insert into store_events values ('u1','app2','store_app_view','2020-01-02 19:10:43');
insert into store_events values ('u1','app2','store_app_download','2020-01-02 19:10:44');


insert into store_events values ('u2','app2','store_app_download','2020-01-02 19:10:25');
insert into store_events values ('u2','app2','store_app_view','2020-01-02 19:10:26');
insert into store_events values ('u2','app2','store_app_install','2020-01-02 19:10:27');
insert into store_events values ('u2','app2','store_app_view','2020-01-02 19:10:30');
insert into store_events values ('u2','app2','store_app_update','2020-01-02 19:10:31');
insert into store_events values ('u2','app2','store_fetch_manifest','2020-01-02 19:10:35');
insert into store_events values ('u2','app2','store_app_view','2020-01-02 19:10:39');
insert into store_events values ('u2','app2','store_app_install','2020-01-02 19:10:40');
insert into store_events values ('u2','app2','store_app_download','2020-01-02 19:10:42');
insert into store_events values ('u2','app2','store_app_view','2020-01-02 19:10:43');
insert into store_events values ('u2','app2','store_app_download','2020-01-02 19:10:44');

*/