"""
预处理代码
"""
###part1:曝光-点击
create table if not EXISTS  bi_ngmm.tem_ch_item_expose_inf
( date_               datetime comment '日期',
  distinct_id         string comment '用户ID',
  micro_page_id       string comment '微页面ID',
  micro_page_name     string comment '微页面名称', 
  micro_cindex        bigint comment '楼层',
  component_name      string comment '组件名称',
  goods_id            string comment '商品ID', 
  platformtype        string comment '平台',
  os                  string comment '操作系统',
  network_type        string comment '联网方式',
  manufacturer        string comment '手机品牌',
  model               string comment '手机型号',
  country             string comment '国家',
  province            string comment '省份',
  city                string comment '城市',
  exposure_time       string comment '曝光时间',
  click_time          string comment '点击时间',
  time_dis            bigint comment '时间间隔',
  tag                 string comment '样本标签' 
)
partitioned by (dt string);

insert OVERWRITE table bi_ngmm.tem_ch_item_expose_inf partition(dt)
select M.*
from 
(
select s2.date_, s2.distinct_id,
       s2.micro_page_id, s2.micro_page_name, s2.micro_cindex, s2.component_name,
       s2.goods_id, s2.platformtype, s2.os, s2.network_type, 
       s2.manufacturer, s2.model, s2.country, s2.province, s2.city,
       s2.exposure_time, s2.click_time, s2.time_dis, 
       case when s2.click_time is not null and s2.time_dis < 30 then 1 else 0 end tag,
       to_char(s2.date_, 'yyyymmdd') dt
from 
   (
       select s1.*,
              rank() over(partition by distinct_id, goods_id, click_time order by time_dis) num
       from
         (
             select t1.*,t2.click_time,
                    case when t2.click_time is not null then 
                    abs(datediff(to_date(t2.click_time, 'yyyy-mm-dd hh:mi:ss.ff3'), to_date(t1.exposure_time, 'yyyy-mm-dd hh:mi:ss.ff3'), 'ff3')) 
                         else null end/1000 time_dis
             from      
               (
                   select date date_, distinct_id,
                          micro_page_id, micro_page_name, micro_cindex, component_name,
                          case when platformType in ('电商小程序') then REGEXP_EXTRACT(micro_component_link,'(id=)(\\d+)(&)',2) 
                                when platformType in ('Android', 'iOS') then REGEXP_EXTRACT(concat(micro_component_link,'/'),'(goods/)(\\d+)(/)',2) 
                                else null end goods_id,
                          substr(time,1,23) exposure_time, 
                          platformType, os, network_type, manufacturer,
				          model, country, province, city 
                   from bi_ngmm.ods_sc_exposure_i_d
                   where event = 'exposureAttributeCollection' 
		                 and pt = ${bdp.system.bizdate}
                         and (micro_component_link like '%details%' or micro_component_link like '%goods/%')
                         and platformType in ('电商小程序', 'Android' ,'iOS')
                         and micro_page_id is not null 
                         and micro_page_id <> 'NULL'
                         and is_login_id = 1 
                         and length(distinct_id) > 2
               )t1
             left join
               (
                   select  t_date date_, distinct_id,
                           micro_page_id, micro_page_name, micro_cindex, component_name,
                           case when platformType in ('电商小程序') then REGEXP_EXTRACT(micro_component_link,'(id=)(\\d+)(&)',2) 
                                when platformType in ('Android', 'iOS') then REGEXP_EXTRACT(concat(micro_component_link,'/'),'(goods/)(\\d+)(/)',2) 
                                else null end goods_id,
                           substr(t_time,1,23) click_time, 
                           platformType  
                   from bi_ngmm.ods_sc_micropage_click_i_d
                   where event = 'micropage_click' 
		                 and pt = ${bdp.system.bizdate}
                         and (micro_component_link like '%details%' or micro_component_link like '%goods/%')
                         and micro_page_id is not null 
                         and micro_page_id <> 'NULL'
                         and platformType in ('电商小程序', 'Android' , 'iOS')
                         and length(distinct_id) > 2 and length(distinct_id) < 11
               )t2
             on t1.distinct_id = t2.distinct_id 
	            and t1.goods_id = t2.goods_id 
                and t1.platformType = t2.platformType 
	            and t1.date_ = t2.date_
                and t1.micro_page_id = t2.micro_page_id 
	            and t1.micro_cindex = t2.micro_cindex 
                and abs(datediff(to_date(t2.click_time, 'yyyy-mm-dd hh:mi:ss.ff3'), to_date(t1.exposure_time, 'yyyy-mm-dd hh:mi:ss.ff3'), 'ff3')/1000) < 30
   )s1
)s2
where s2.click_time is null or (s2.click_time is not null and s2.num = 1)
)M 
left join 
(
  select distinct_id, 
         t_date date_, 
         platformType, 
         micro_page_id, 
         case when platformType in ('电商小程序') then REGEXP_EXTRACT(micro_component_link,'(id=)(\\d+)(&)',2) 
              when platformType in ('Android', 'iOS') then REGEXP_EXTRACT(concat(micro_component_link,'/'),'(goods/)(\\d+)(/)',2) 
              else null end goods_id,
         max(substr(t_time,1,23)) last_click_time
  from bi_ngmm.ods_sc_micropage_click_i_d
  where event = 'micropage_click' 
		and pt = ${bdp.system.bizdate}
        and (micro_component_link like '%details%' or micro_component_link like '%goods/%')
        and platformType in ('电商小程序', 'Android', 'iOS')
        and micro_page_id is not null
        and micro_page_id <> 'NULL'
        and length(distinct_id) > 2 and length(distinct_id) < 11
  group by distinct_id, 
           t_date, 
           platformType, 
           micro_page_id, 
           case when platformType in ('电商小程序') then REGEXP_EXTRACT(micro_component_link,'(id=)(\\d+)(&)',2) 
                when platformType in ('Android', 'iOS') then REGEXP_EXTRACT(concat(micro_component_link,'/'),'(goods/)(\\d+)(/)',2) 
                else null end
)N
on M.distinct_id = N.distinct_id 
   and M.goods_id = N.goods_id 
   and M.platformType = N.platformType 
   and M.date_ = N.date_
where M.goods_id is not null 
      and (N.distinct_id is null or datediff(to_date(N.last_click_time, 'yyyy-mm-dd hh:mi:ss.ff3'), to_date(M.exposure_time, 'yyyy-mm-dd hh:mi:ss.ff3'), 'ff3')/1000 > -30)
;


##part2：点击-购买
CREATE TABLE if not exists bi_ngmm.app_alg_cvr_within_5days_user_click_istrade_s_d 
(
	`user_id` STRING comment '用户ID',
	`goods_id` BIGINT comment '商品ID',
	`act_date` STRING comment '浏览日期',
	`act_time` DATETIME comment '浏览时间',
	`goods_price` DOUBLE comment '浏览时商品价格',
	`platformtype` STRING comment '平台类型',
	`network_type` STRING comment '联网方式',

	`manufacturer` STRING comment '手机品牌',
	`t_country` STRING comment '国家',
	`t_province` STRING comment '省份',
	`t_city` STRING comment '市',
	`order_time` DATETIME comment '下单时间',
	`dis` DOUBLE comment '最后一次浏览－下单时间间隔',
	`pay_price` DOUBLE comment '实付价格',
	`istrade` BIGINT comment '是否在５日内下单'
)
partitioned by (pt string)
;

insert OVERWRITE table bi_ngmm.app_alg_cvr_within_5days_user_click_istrade_s_d  partition(pt)
 select n1.*, n2.order_time, 
        n2.dis,
        n2.pay_price,
        case when n2.user_id is not null then 1 else 0 end istrade,
        to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-5,'dd'),'yyyymmdd') pt
 from 
    (
        select cast(distinct_id as string) as user_id, 
               cast(commodity_id as bigint) as goods_id, 
               to_char(to_date(t_date,'yyyy-mm-dd hh:mi:ss'),'yyyymmdd') as act_date, 
               to_date(substr(t_time,1,23),'yyyy-mm-dd hh:mi:ss.ff3') as act_time, 
               cast(commodity_price as double) goods_price,
               tolower(platformtype) platformtype, 
               tolower(t_network_type) network_type, 
               tolower(t_manufacturer) manufacturer, 
               t_country, t_province, t_city
        from bi_ngmm.ods_sc_user_act_event_i_d
        where pt = to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-5,'dd'),'yyyymmdd')
              and event = 'viewCommodityDetail'
              and length(distinct_id) > 2 and length(distinct_id) < 10
    )n1
    left join
     (
         select m1.*,
                m2.act_time, 
                round(datediff(m1.order_time, m2.act_time, 'mi')/60, 2) dis,
                row_number() over(PARTITION by m1.user_id, m1.goods_id, m1.order_time order by m2.act_time desc) ind
         from 
           (
               SELECT cast(t.user_id as string) as user_id, 
                      cast(t1.item_id as bigint) as goods_id, 
                      t.order_date, 
                      t.order_time ,
                      t1.payment/100 pay_price,
                      '4' act_type
               from
                 ( 
                     select user_id
                            , created_time as order_time
                            , to_char(created_time, 'yyyymmdd') as order_date
                            , trade_id
                            , payment
                            , trade_state
                     from bi_ngmm.ods_trade_trade_s_d 
                     where pt = '${bdp.system.bizdate}'
                           and mall_id in (1,2) 
                           and trade_state in (3,6,7)
                           and created_time >= dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-5,'dd')
                 ) t
               join 
                ( 
                     select user_id
                            , item_id
                            , trade_id
                            , payment
                     from bi_ngmm.ods_trade_trade_order_s_d
                     where pt = '${bdp.system.bizdate}'
                           and created_time >= dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-5,'dd')
                ) t1
               on t.trade_id = t1.trade_id
           )m1 
         join 
          (
              select cast(distinct_id as string) as user_id, 
                     cast(commodity_id as bigint) as goods_id, 
                     to_char(to_date(t_date,'yyyy-mm-dd hh:mi:ss'),'yyyymmdd') as act_date, 
                     to_date(substr(t_time,1,23),'yyyy-mm-dd hh:mi:ss.ff3') as act_time, 
                     cast(commodity_price as double) goods_price,
                     '1' as act_type
              from bi_ngmm.ods_sc_user_act_event_i_d
              where pt >= to_char(dateadd(to_date('${bdp.system.bizdate}','yyyymmdd'),-5,'dd'),'yyyymmdd')
                    and pt <= '${bdp.system.bizdate}'
                    and event = 'viewCommodityDetail'
                    and length(distinct_id) > 2 and length(distinct_id) < 10
           )m2 
         on m1.user_id = m2.user_id 
            and m1.goods_id = m2.goods_id
            and datediff(m1.order_time, m2.act_time, 'mi') >= 0
            and datediff(m1.order_time, m2.act_time, 'mi') <= 7200
    )n2
    on n1.user_id=n2.user_id 
       and n1.act_time = n2.act_time 
       and n1.goods_id = n2.goods_id 
       and n2.ind = 1
order by n1.act_time
;

##part3:数据合并
select n1.*,
       n2.create_time,
       n2.first_subscribe_time,
       n2.app_register_time,
       n2.first_ec_order_time,
       n2.last_ec_order_time,
       n2.fixed_baby_birthday,
       n2.rfm
from 
(
    select 
        distinct_id user_id,
        goods_id,
        to_char(date_,'yyyymmdd') act_date,
        'exposure' act_tag,
        to_date(exposure_time, 'yyyy-mm-dd hh:mi:ss.ff3') act_time,
        tolower(platformtype) platformtype,
        tolower(network_type) network_type,
        tolower(manufacturer) manufacturer,
        country,
        province,
        city,
        to_date(click_time, 'yyyy-mm-dd hh:mi:ss.ff3') trans_time, 
        cast(time_dis as double) dis,
        cast(tag as bigint) tag,
        dt pt 
    from bi_ngmm.tem_ch_item_expose_inf
    where dt = 20200517
    union all
    select 
        user_id,
        cast(goods_id as string) goods_id,
        act_date,
        'click' act_tag,
        act_time,
        platformtype,
        network_type,
        manufacturer,
        t_country country,
        t_province province,
        t_city city,
        order_time trans_time,
        dis,
        istrade tag,
        pt
    from bi_ngmm.app_alg_cvr_within_5days_user_click_istrade_s_d
    where pt = 20200517
)n1
join bi_ngmm.app_alg_cvr_all_user_base_inf_s_d n2
on n1.user_id = n2.user_id and n1.pt = n2.pt 
;
