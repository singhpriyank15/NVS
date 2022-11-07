-- Address with Null MDM ID & Null Addr ODW ID in Veeva - Present in ODS with Non-Null MDM addr ID & Non-null Addr ODW ID. Addreses present in ODS & Veeva on same account



with missing_data as (
select * from all_all_r_all_bkp.hcp_addr_cleanup_case1 bkp where exists( select 1 from all_all_b_usa_crmods.ods_contacts_addr con where concat('C',cast(con.contacts_odw_id as String)) = bkp.account_odw__c and bkp.id = con.sf_id and con.mdm_addr_id is not null and con.contacts_addr_odw_id is not null) and  bkp.address_mdm_id__c is null and bkp.address_odw__c is  null ),upd_data as (
select md.id,ca.mdm_addr_id,concat('CA',cast(ca.contacts_addr_odw_id as string)) addr_odw_id,ca.sf_id,concat('C',cast(ca.contacts_odw_id as string)) contact_odw_id from missing_data md join all_all_b_usa_crmods.ods_contacts_addr ca on concat('C',cast(ca.contacts_odw_id as String)) = md.account_odw__c and md.id = ca.sf_id where ca.mdm_addr_id is not null and ca.contacts_addr_odw_id is not null ),comp_data as (
select upd.*,addr1.address_odw__c,addr1.id as id1,acct.account_odw__c,addr.name,addr1.name as name1,addr.city_vod__c,addr1.city_vod__c city1,addr.state_vod__c,addr1.state_vod__c as state1,addr.zip_vod__c ,addr1.zip_vod__c as zip1 from upd_Data upd left join ph_com_p_usa_veeva.address_vod__c addr on addr.id=upd.id left join ph_com_p_usa_veeva.address_vod__c addr1 on nvl(upd.addr_odw_id,'@') = nvl(addr1.address_odw__c,'@') left join ph_com_p_usa_veeva.account acct on acct.id = addr.account_vod__c and acct.ispersonaccount = true and acct.isdeleted=false and acct.recordtypeid='0121O000001CG6UQAW')

select cd.*,e.nvs_core_address__c,c.parent_address__c from comp_data cd left join ph_com_p_usa_veeva.call2_vod__c c on c.parent_address__c = cd.id1 left join ph_com_p_usa_veeva.em_event_vod__c e on e.nvs_core_address__c=cd.id1


addr.name,addr1.name,addr.city_vod__c,addr1.city_vod__c,addr.state_vod__c,addr1.state_vod__c,addr.zip_vod__c,addr1.zip_vod__c



select * from upd_Data upd where not exists ( select 1 from ph_com_p_usa_veeva.address_vod__c addr  where nvl(upd.addr_odw_id,'@') = nvl(addr.address_odw__c,'@'))


select * from all_all_r_all_bkp.hcp_addr_cleanup_case1 bkp where address_mdm_id__c is null and address_odw__c is null and addr_sf_sf_id is not null and sf_id is not null and mdm_addr_id is not null and acc_od_account_check = true and acc_od_mdm_addr_id_check = false and acc_od_odw_id_check = false and addr_sf_odw_id_check is false and addr_mdm_addr_id_check is false

IF(AND(C12="NULL",D12="NULL",F12<>"NULL",M12=TRUE,I12<>"NULL",K12=FALSE,L12=FALSE,W12<>"NULL",Y12=FALSE,Z12=FALSE,AA12=TRUE),"Case1.4")


------------------------------------------------------------------------------------

with upd_data as (
select id as upd_data_id,hcp_odw_id ,sf_id,mdm_addr_id,addr_odw_id from all_all_r_all_bkp.hcp_addr_cleanup_case1 bkp where address_mdm_id__c is null and address_odw__c is null and addr_sf_sf_id is not null and sf_id is not null and mdm_addr_id is not null and acc_od_account_check = true and acc_od_mdm_addr_id_check = false and acc_od_odw_id_check = false and addr_sf_odw_id_check is false and addr_mdm_addr_id_check is false),cmp_data as (
select upd.*,addr.address_odw__c,addr.id,acct.account_odw__c,addr.address_mdm_id__c from upd_Data upd left join ph_com_p_usa_veeva.address_vod__c addr on nvl(upd.addr_odw_id,'@') = nvl(addr.address_odw__c,'@') left join ph_com_p_usa_veeva.account acct on acct.id = addr.account_vod__c and acct.ispersonaccount = true and acct.isdeleted=false and acct.recordtypeid='0121O000001CG6UQAW'),upd_name as (
select u.name as created_name,upd.* from cmp_data upd left join (select id,createdbyid from ph_com_p_usa_veeva.address_vod__c) addr on addr.id=upd.upd_data_id left join (select name,id from ph_com_p_usa_veeva.user) u on addr.createdbyid=u.id),cmp_data_addr_detais as (
select cd.*,addr.name,addr1.name as name1,addr.city_vod__c,addr1.city_vod__c city1,addr.state_vod__c,addr1.state_vod__c as state1,addr.zip_vod__c ,addr1.zip_vod__c as zip1 from upd_name cd left join ph_com_p_usa_veeva.address_vod__c addr  on addr.id=cd.sf_id left join ph_com_p_usa_veeva.address_vod__c addr1 on cd.id=addr1.id), evt_data as (
select e.nvs_core_address__c,count(1) as evt_count from cmp_data_addr_detais cda join ph_com_p_usa_veeva.em_event_vod__c e   on e.nvs_core_address__c=cda.sf_id group by e.nvs_core_address__c union select e.nvs_core_address__c,count(1) as evt_count from cmp_data_addr_detais cda join ph_com_p_usa_veeva.em_event_vod__c e on e.nvs_core_address__c=cda.id group by e.nvs_core_address__c ), calls_data as (
select c.parent_address_vod__c,count(1) as call_count from cmp_data_addr_detais cda left join ph_com_p_usa_veeva.call2_vod__c c on c.parent_address_vod__c  = cda.sf_id group by c.parent_address_vod__c union select c.parent_address_vod__c,count(1) as call_count from cmp_data_addr_detais cda  join ph_com_p_usa_veeva.call2_vod__c c on c.parent_address_vod__c = cda.id group by c.parent_address_vod__c)
select cda.* ,e.nvs_core_address__c as evt_ods_sf_id,nvl(e.evt_count,0) event_cnt,e1.nvs_core_address__c as evt_veeva_sf_id,nvl(e1.evt_count,0) event_cnt,c.parent_address_vod__c as call_ods_sf_id,nvl(c.call_count,0) as call_cnt,c1.parent_address_vod__c as call_veeva_sf_id,nvl(c1.call_count,0) as call_cnt from cmp_data_addr_detais cda left join calls_data c on c.parent_address_vod__c  = cda.sf_id left join evt_data e on e.nvs_core_address__c=cda.sf_id left join calls_data c1 on c1.parent_address_vod__c  = cda.id left join evt_data e1 on e1.nvs_core_address__c=cda.id 

select upd.upd_data_id,u.name from final_data upd left join ph_com_p_usa_veeva.address_vod__c addr on addr.id=upd.upd_data_id left join ph_com_p_usa_veeva.user u on addr.createdbyid=u.id

select upd.upd_data_id,u.name from upd_Data upd left join ph_com_p_usa_veeva.address_vod__c addr on addr.id=upd.upd_data_id left join ph_com_p_usa_veeva.user u on addr.createdbyid=u.id