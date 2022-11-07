-- HCP ADDR CLEANUP CASE 1.4 : 2.Update lastmodifieddate of SF_IDs in Veeva with Transaction

spark.sql("""with upd_data as (
select id as upd_data_id,hcp_odw_id ,sf_id,mdm_addr_id,addr_odw_id from all_all_r_all_bkp.hcp_addr_cleanup_case1 bkp where address_mdm_id__c is null and address_odw__c is null and addr_sf_sf_id is not null and sf_id is not null and mdm_addr_id is not null and acc_od_account_check = true and acc_od_mdm_addr_id_check = false and acc_od_odw_id_check = false and addr_sf_odw_id_check = false and addr_mdm_addr_id_check = false),cmp_data as (
select upd.*,addr.address_odw__c,addr.id,acct.account_odw__c,addr.address_mdm_id__c from upd_Data upd left join ph_com_p_usa_veeva.address_vod__c addr on nvl(upd.addr_odw_id,'@') = nvl(addr.address_odw__c,'@') left join ph_com_p_usa_veeva.account acct on acct.id = addr.account_vod__c and acct.ispersonaccount = true and acct.isdeleted=false and acct.recordtypeid='0121O000001CG6UQAW'),evt_data as (
select e.nvs_core_address__c,count(1) as evt_count from cmp_data cda join ph_com_p_usa_veeva.em_event_vod__c e   on e.nvs_core_address__c=cda.sf_id group by e.nvs_core_address__c union select e.nvs_core_address__c,count(1) as evt_count from cmp_data cda join ph_com_p_usa_veeva.em_event_vod__c e on e.nvs_core_address__c=cda.id group by e.nvs_core_address__c ), calls_data as (
select c.parent_address_vod__c,count(1) as call_count from cmp_data cda left join ph_com_p_usa_veeva.call2_vod__c c on c.parent_address_vod__c  = cda.sf_id group by c.parent_address_vod__c union select c.parent_address_vod__c,count(1) as call_count from cmp_data cda  join ph_com_p_usa_veeva.call2_vod__c c on c.parent_address_vod__c = cda.id group by c.parent_address_vod__c),trans_data as (
select cda.* ,e.nvs_core_address__c as evt_ods_sf_id,nvl(e.evt_count,0) event_cnt_ods,e1.nvs_core_address__c as evt_veeva_sf_id,nvl(e1.evt_count,0) event_cnt_veeva,c.parent_address_vod__c as call_ods_sf_id,nvl(c.call_count,0) as call_cnt_ods,c1.parent_address_vod__c as call_veeva_sf_id,nvl(c1.call_count,0) as call_cnt_veeva from cmp_data cda left join calls_data c on c.parent_address_vod__c  = cda.sf_id left join evt_data e on e.nvs_core_address__c=cda.sf_id left join calls_data c1 on c1.parent_address_vod__c  = cda.id left join evt_data e1 on e1.nvs_core_address__c=cda.id ),final_data as (
select *,case when trim(nvl(mdm_addr_id,'@'))=trim(nvl(address_mdm_id__c,'@')) then true else false end as addr_mdm_id_check,case when trim(nvl(address_odw__c,'@'))=trim(nvl(addr_odw_id,'@')) then true else false end as addr_odw_id_check,case when trim(nvl(account_odw__c,'@'))=trim(nvl(hcp_odw_id,'@')) then true else false end as account_check,case when trim(nvl(sf_id,'@'))=trim(nvl(id,'@')) then true else false end as sf_id_check from trans_data)

select upd_data_id as id,call_cnt_ods,event_cnt_ods from final_data where (call_cnt_ods <> 0 or event_cnt_ods <> 0 )and account_check = true and addr_mdm_id_check = true and addr_odw_id_check = true""").createOrReplaceTempView("final_data")




 final_data.repartition(1).write.option("header","true").option("sep",",").mode("overwrite").csv("/HDFS PATH/data")  -- Give your local HDFS path. Share & use the file for Dataloader

