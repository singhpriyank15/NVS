-------------------  Transaction present on Both ID  -----------------------

-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_contacts_addr_t like ALL_ALL_B_USA_CRMODS.ods_contacts_addr")

--BACKUP

spark.sql("create table if not exists all_all_r_all_bkp.ods_contacts_addr_case1_11 like ALL_ALL_B_USA_CRMODS.ods_contacts_addr")
spark.sql("insert into all_all_r_all_bkp.ods_contacts_addr_case1_11 select * from ALL_ALL_B_USA_CRMODS.ods_contacts_addr")

--creating base table

spark.sql("""with upd_data as (
select id as upd_data_id,address_mdm_id__c as upd_addr_mdm_id,address_odw__c as upd_addr_odw__c,account_odw__c as upd_account_odw__c,addr_o_sf_id as upd_sf_id from all_all_r_all_bkp.hcp_addr_cleanup_case1 bkp where address_mdm_id__c is not null and address_odw__c is not null and sf_id is null and addr_o_addr_odw_id is not null and addr_o_sf_id is not null and addr_od_mdm_addr_id_check = TRUE and addr_od_sf_id_check = FALSE and addr_od_account_check =TRUE and addr_sf_sf_id is null),cmp_data as (
select upd.*,addr.id,acct.id as account_sfid,addr.address_odw__c,acct.account_odw__c,addr.address_mdm_id__c from upd_Data upd left join ph_com_p_usa_veeva.address_vod__c addr on nvl(upd.upd_sf_id,'@') = nvl(addr.id,'@') left join ph_com_p_usa_veeva.account acct on acct.id = addr.account_vod__c and acct.ispersonaccount = true and acct.isdeleted=false and acct.recordtypeid='0121O000001CG6UQAW'),upd_name as (
select u.name as created_name_upd,upd.*,u1.name as created_name from cmp_data upd left join (select id,createdbyid from ph_com_p_usa_veeva.address_vod__c) addr on addr.id=upd.upd_data_id left join (select name,id from ph_com_p_usa_veeva.user) u on addr.createdbyid=u.id left join (select id,createdbyid from ph_com_p_usa_veeva.address_vod__c) addr1 on addr1.id=upd.id left join (select name,id from ph_com_p_usa_veeva.user) u1 on addr1.createdbyid=u1.id),
cmp_data_addr_detais as (
select cd.*,addr.name,addr1.name as name1,addr.city_vod__c,addr1.city_vod__c city1,addr.state_vod__c,addr1.state_vod__c as state1,addr.zip_vod__c ,addr1.zip_vod__c as zip1 from upd_name cd left join ph_com_p_usa_veeva.address_vod__c addr  on addr.id=cd.upd_data_id left join ph_com_p_usa_veeva.address_vod__c addr1 on cd.id=addr1.id), evt_data as (
select e.nvs_core_address__c,count(1) as evt_count from cmp_data_addr_detais cda join ph_com_p_usa_veeva.em_event_vod__c e   on e.nvs_core_address__c=cda.upd_data_id group by e.nvs_core_address__c union select e.nvs_core_address__c,count(1) as evt_count from cmp_data_addr_detais cda join ph_com_p_usa_veeva.em_event_vod__c e on e.nvs_core_address__c=cda.id group by e.nvs_core_address__c ), calls_data as (
select c.parent_address_vod__c,count(1) as call_count from cmp_data_addr_detais cda left join ph_com_p_usa_veeva.call2_vod__c c on c.parent_address_vod__c  = cda.upd_data_id group by c.parent_address_vod__c union select c.parent_address_vod__c,count(1) as call_count from cmp_data_addr_detais cda  join ph_com_p_usa_veeva.call2_vod__c c on c.parent_address_vod__c = cda.id group by c.parent_address_vod__c),trans_data as (
select cda.* ,e.nvs_core_address__c as evt_upd_sf_id,nvl(e.evt_count,0) event_cnt_upd,e1.nvs_core_address__c as evt_veeva_sf_id,nvl(e1.evt_count,0) event_cnt_veeva,c.parent_address_vod__c as call_upd_sf_id,nvl(c.call_count,0) as call_cnt_upd,c1.parent_address_vod__c as call_veeva_sf_id,nvl(c1.call_count,0) as call_cnt_veeva from cmp_data_addr_detais cda left join calls_data c on c.parent_address_vod__c  = cda.upd_data_id left join evt_data e on e.nvs_core_address__c=cda.upd_data_id left join calls_data c1 on c1.parent_address_vod__c  = cda.id left join evt_data e1 on e1.nvs_core_address__c=cda.id), final_data as (select *,case when trim(nvl(upd_addr_mdm_id,'@'))=trim(nvl(address_mdm_id__c,'@')) then true else false end as addr_mdm_id_check,case when trim(nvl(address_odw__c,'@'))=trim(nvl(upd_addr_odw__c,'@')) then true else false end as addr_odw_id_check,case when trim(nvl(account_odw__c,'@'))=trim(nvl(upd_account_odw__c,'@')) then true else false end as account_check,case when trim(nvl(upd_sf_id,'@'))=trim(nvl(id,'@')) then true else false end as sf_id_check from trans_data), both_trans as (select upd_data_id,upd_sf_id,id from final_data where sf_id_check = true and (call_cnt_upd <> 0 or event_cnt_upd <> 0) and (call_cnt_veeva <> 0 or event_cnt_veeva <> 0))

select id,concat('CA',cast((989999999999 + rn) as String)) address_odw__c  from (select id,row_number() over (order by id) as rn from both_trans) a""").createOrReplaceTempView("final_data")


val df = spark.sql("select * from final_data")

df.repartition(1).write.option("header","true").option("sep",",").mode("overwrite").csv("/HDFS PATH/data")  -- Give your local HDFS path. Share & use the file for Dataloader