spark.sparkContext.setLogLevel("ERROR")

spark.sql("""create table all_all_r_all_bkp.ods_acct_addr_3sep22_Inserts like all_all_b_usa_crmods.ods_acct_addr""")


spark.sql("""insert into all_all_r_all_bkp.ods_acct_addr_3sep22_Inserts select * from all_all_b_usa_crmods.ods_acct_addr""")

spark.sql("""with union_data as (select * from all_all_r_all_bkp.hco_addr_base_query_case1 union select * from all_all_r_all_bkp.hco_addr_base_query_case2), addr_dup as (select address_mdm_id__c,count(1) from union_data group by address_mdm_id__c having count(1) =1), case1a as (select id,address_mdm_id__c,address_odw__c,account_odw__c from all_all_r_all_bkp.hco_addr_base_query_case1 where sf_id is null and addr_o_addr_odw_id is null and addr_sf_sf_id is null), req_data as ( select u.* from union_data u left anti join case1a c on u.id=c.id and u.address_odw__c=c.address_odw__c  and u.account_odw__c=c.account_odw__c)

select distinct id,address_odw__c,address_mdm_id__c,account_odw__c from req_data""").createOrReplaceTempView("final_data")

-- Insert in acct_addr where addr_mdm_id is NULL

spark.sql("""INSERT INTO all_all_b_usa_crmods.ODS_ACCT_ADDR select null acct_phys_addr_line_3,addr.address_line_2_vod__c acct_phys_addr_line_2,addr.name acct_phys_addr_line_1,null mdm_valid_addr_ind,addr.home_vod__c home,addr.include_in_territory_assignment_vod__c inc_in_terr_asgnmt,addr.primary_vod__c primary,addr.shipping_vod__c shipping,null mailing,null acct_name,substring(veeva.account_odw__c,2) acct_odw_id,null cam_acct_key,null sf_owner_id,'FALSE' sf_is_deleted,addr.id sf_id,substring(veeva.address_odw__c,3) acct_addr_odw_id,addr.best_times_vod__c best_times,upper(addr.cass_certified__c) cass_certified,null mdm_src_addr_type,null mdm_cust_skey,null mdm_addr_src_sys,null edge_id,veeva.address_mdm_id__c mdm_addr_id,addr.billing_vod__c billing,null sonic_id,null business,null cust_addr_skey,null acct_phys_zip2,null acct_phys_zip1,'2345' batch_id_insert,'VOD' rec_insert_by,current_timestamp() rec_modify_date,current_timestamp() rec_insert_date,ADDr.inactive_vod__c inactive,addr.office_notes_text_area_nov__c office_notes,addr.staff_notes_text_area_nov__c staff_notes,addr.source_vod__c source,addr.fax_2_vod__c acct_fax_2,addr.fax_vod__c acct_fax_1,addr.phone_2_vod__c acct_phn_2,addr.phone_vod__c acct_phn_1,addr.zip_4_vod__c zip_4,null cntry_code,null acct_post_zip2,null acct_post_zip1,null acct_post_state,null acct_post_county,null acct_post_country,null acct_post_city,null acct_post_addr_line3,null acct_post_addr_line2,null acct_post_addr_line1,addr.zip_vod__c acct_phys_zip,addr.state_vod__c acct_phys_state,null acct_phys_county,null acct_phys_country,addr.city_vod__c acct_phys_city,null siebel_id,'2345' batch_id_update,'VOD' rec_modify_by,addr.appt_required_vod__c appt_required_vod__c,addr.nvs_core_secondary_license__c nvs_core_secondary_license__c,addr.map_vod__c maps,addr.sample_status_vod__c sample_status_vod__c,addr.sample_send_status_vod__c sample_send_status_vod__c,addr.nvs_core_novartis_unique_id__c nvs_core_novartis_unique_id__c from ph_com_p_usa_veeva.Address_vod__c addr join (select * from final_data where address_mdm_id__c is null) veeva on trim(addr.id)=trim(veeva.id)  join all_all_b_usa_crmods.ods_accts acct on concat('A', cast(acct.acct_odw_id as string)) = veeva.account_odw__c""") 


-- Insert in acct_addr where addr_mdm_id is not NULL

spark.sql("insert into  all_all_b_usa_crmods.ODS_ACCT_ADDR select depBlue.acct_phys_addr_line_3 acct_phys_addr_line_3,depBlue.acct_phys_addr_line_2,depBlue.acct_phys_addr_line_1,depBlue.mdm_valid_addr_ind,addr.home_vod__c home,addr.include_in_territory_assignment_vod__c inc_in_terr_asgnmt,addr.primary_vod__c primary,addr.shipping_vod__c shipping,a.mailing,null acct_name,depBlue.acct_odw_id acct_odw_id,null cam_acct_key,null sf_owner_id,'FALSE' sf_is_deleted,addr.id sf_id,substring(addr.address_odw__c,3) acct_addr_odw_id,addr.best_times_vod__c best_times,upper(addr.cass_certified__c) cass_certified,null mdm_src_addr_type,null mdm_cust_skey,null mdm_addr_src_sys,null edge_id,depBlue.mdm_addr_id,addr.billing_vod__c billing,null sonic_id,a.business,null cust_addr_skey,null acct_phys_zip2,null acct_phys_zip1,'2345' batch_id_insert,'VOD' rec_insert_by,current_timestamp() rec_modify_date,current_timestamp() rec_insert_date,ADDr.inactive_vod__c inactive,addr.office_notes_text_area_nov__c office_notes,addr.staff_notes_text_area_nov__c staff_notes,addr.source_vod__c source,addr.fax_2_vod__c acct_fax_2,addr.fax_vod__c acct_fax_1,addr.phone_2_vod__c acct_phn_2,addr.phone_vod__c acct_phn_1,depBlue.zip_4,null cntry_code,null acct_post_zip2,null acct_post_zip1,null acct_post_state,null acct_post_county,null acct_post_country,null acct_post_city,null acct_post_addr_line3,null acct_post_addr_line2,null acct_post_addr_line1,depBlue.acct_phys_zip,depBlue.acct_phys_state,depBlue.acct_phys_county,depBlue.acct_phys_country,depBlue.acct_phys_city,null siebel_id,null batch_id_update,'VOD' rec_modify_by,addr.appt_required_vod__c appt_required_vod__c,addr.nvs_core_secondary_license__c nvs_core_secondary_license__c,addr.map_vod__c maps,addr.sample_status_vod__c sample_status_vod__c,addr.sample_send_status_vod__c sample_send_status_vod__c,depBlue.customer_id nvs_core_novartis_unique_id__c from ph_com_p_usa_veeva.Address_vod__c addr join (select * from final_data where address_mdm_id__c is not null) rod on trim(addr.id) = trim(rod.id)  JOIN (select account_odw__c,id,nvs_core_novartis_unique_id__c from ph_com_p_usa_veeva.account act where act.recordtypeid not in ('01270000000Mto8AAC','0120S000000cbGUQAY') ) act on  addr.account_vod__c=act.id join (SELECT DISTINCT trim(customer_id) customer_id,trim(addr_id) mdm_addr_id,acct.acct_odw_id acct_odw_id,addr_line_1 acct_phys_addr_line_1,addr_line_2 acct_phys_addr_line_2 ,addr_line_3 acct_phys_addr_line_3,city acct_phys_city,state acct_phys_state,zip acct_phys_zip,zip_ext zip_4,province acct_phys_county,depBlue.country acct_phys_country,verification_status mdm_valid_addr_ind,addr_status,status_reason FROM ALL_ALL_E_GBL_CUSTOMER.idl_mdm_address depBlue left anti join (select addr_id,customer_id from ALL_ALL_E_GBL_CUSTOMER.idl_mdm_address_type okl where trim(upper(okl.addr_type)) ='PO BOX' and okl.status='A') rec on trim(rec.addr_id)=trim(depBlue.addr_id) and trim(rec.customer_id)=trim(depBlue.customer_id) JOIN ALL_ALL_B_USA_CRMODS.ODS_ACCTS acct ON trim(depBlue.customer_id)=trim(acct.mdm_party_id) left anti join ALL_ALL_B_USA_CRMODS.ODS_ACCT_ADDR odsAcct on trim(depBlue.addr_id)=trim(odsAcct.mdm_addr_id) and trim(upper(depBlue.addr_type)) ='HCO' )depBlue  ON  trim(depBlue.mdm_addr_id)=trim(addr.address_mdm_id__c) left anti JOIN ALL_ALL_B_USA_CRMODS.ODS_ACCT_ADDR odsAcct ON trim(depBlue.mdm_addr_id)=trim(odsAcct.mdm_addr_id) left outer  join  (select trim(prev_mdm_addr_id) prev_mdm_addr_id from ALL_ALL_E_GBL_CUSTOMER.idl_addr_merge_history ) looserAddr on trim(looserAddr.prev_mdm_addr_id)=trim(depBlue.mdm_addr_id) left outer join (select trim(prev_mdm_id) prev_mdm_id from ALL_ALL_E_GBL_CUSTOMER.idl_merge_history) looserProf on trim(looserProf.prev_mdm_id)=trim(depBlue.customer_id) left outer join (select addr_id,customer_id,max(case when trim(upper(addr_type)) = 'MAILING' then 'TRUE' else 'FALSE' end ) as mailing,max(case when trim(upper(addr_type)) = 'COMMERCIAL' then 'TRUE' else 'FALSE'  end) as business FROM ALL_ALL_E_GBL_CUSTOMER.idl_mdm_address_type where trim(upper(status)) = 'A' group by addr_id,customer_id ) a on trim(a.addr_id)=trim(depBlue.mdm_addr_id) and trim(a.customer_id)=trim(depBlue.customer_id) where LastModifiedById not in('00570000001WPHhAAO')")











