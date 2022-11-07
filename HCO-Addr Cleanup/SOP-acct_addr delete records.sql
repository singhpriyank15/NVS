spark.sparkContext.setLogLevel("ERROR")

spark.sql("""create table all_all_r_all_bkp.ods_acct_addr_29thaug22_case2_1 like all_all_b_usa_crmods.ods_acct_addr""")


spark.sql("""insert into all_all_r_all_bkp.ods_acct_addr_29thaug22_case2_1 select * from all_all_b_usa_crmods.ods_acct_addr""")

spark.sql("""with union_data as (select * from all_all_r_all_bkp.hco_addr_base_query_case1 union select * from all_all_r_all_bkp.hco_addr_base_query_case2), addr_dup as (select address_mdm_id__c,count(1) from union_data group by address_mdm_id__c having count(1) =1), case1a as (select id,address_mdm_id__c,address_odw__c,account_odw__c from all_all_r_all_bkp.hco_addr_base_query_case1 where sf_id is null and addr_o_addr_odw_id is null and addr_sf_sf_id is null), req_data as ( select u.* from union_data u left anti join case1a c on u.id=c.id and u.address_odw__c=c.address_odw__c  and u.account_odw__c=c.account_odw__c)

select ods.* from all_all_b_usa_crmods.ods_acct_addr ods left anti join req_data r on r.id = ods.sf_id or r.address_mdm_id__c=ods.mdm_addr_id or r.address_odw__c=trim(concat('AA',cast(ods.acct_addr_odw_id as string)))""").createOrReplaceTempView("final_data")

-- Delete from acct_addr

spark.sql("""Insert into singhp2j.ODS_ACCT_ADDR_H select home, inc_in_terr_asgnmt, primary, shipping, mailing, acct_name, acct_odw_id, cam_acct_key, sf_owner_id, sf_is_deleted, sf_id, acct_addr_odw_id, best_times, cass_certified, mdm_src_addr_type, mdm_cust_skey, mdm_addr_src_sys, edge_id, mdm_addr_id, business, billing, cust_addr_skey, acct_phys_zip2, acct_phys_zip1, batch_id_insert, rec_insert_date, rec_modify_date, rec_insert_by,current_timestamp eff_end_date, eff_start_date, inactive, office_notes, staff_notes, source, acct_fax_2, acct_fax_1, acct_phn_2, acct_phn_1, zip_4, cntry_code, acct_post_zip2, acct_post_zip1, acct_post_state, acct_post_county, acct_post_country, acct_post_city, acct_post_addr_line_3, acct_post_addr_line_2, acct_post_addr_line_1, acct_phys_zip, acct_phys_state, acct_phys_county, acct_phys_country, acct_phys_city, acct_phys_addr_line_3, acct_phys_addr_line_2, acct_phys_addr_line_1, mdm_valid_addr_ind, siebel_id, batch_id_update, rec_modify_by,appt_required_vod__c,nvs_core_secondary_license__c,maps,sample_status_vod__c,sample_send_status_vod__c,nvs_core_novartis_unique_id__c FROM(select acctAddr.acct_addr_odw_id, acctAddr.sf_id, acctAddr.sf_is_deleted, acctAddr.sf_owner_id, acctAddr.cam_acct_key, acctAddr.acct_odw_id, acctAddr.acct_name, acctAddr.mailing, acctAddr.shipping, acctAddr.primary, acctAddr.inc_in_terr_asgnmt, acctAddr.home, acctAddr.mdm_valid_addr_ind, acctAddr.acct_phys_addr_line_1, acctAddr.acct_phys_addr_line_2, acctAddr.acct_phys_addr_line_3, acctAddr.acct_phys_city, acctAddr.acct_phys_country, acctAddr.acct_phys_county, acctAddr.acct_phys_state, acctAddr.acct_phys_zip, acctAddr.acct_post_addr_line1 acct_post_addr_line_1, acctAddr.acct_post_addr_line2 acct_post_addr_line_2, acctAddr.acct_post_addr_line3 acct_post_addr_line_3, acctAddr.acct_post_city, acctAddr.acct_post_country, acctAddr.acct_post_county, acctAddr.acct_post_state, acctAddr.acct_post_zip1, acctAddr.acct_post_zip2, acctAddr.cntry_code, acctAddr.zip_4, acctAddr.acct_phn_1, acctAddr.acct_phn_2, acctAddr.acct_fax_1, acctAddr.acct_fax_2, acctAddr.source, acctAddr.staff_notes, acctAddr.office_notes, acctAddr.inactive,NVL(acctAddr.rec_insert_date, acctAddr.rec_modify_date)eff_start_date, acctAddr.rec_insert_date, acctAddr.rec_modify_date, acctAddr.rec_insert_by, acctAddr.rec_modify_by, acctAddr.batch_id_insert, acctAddr.batch_id_update, acctAddr.acct_phys_zip1, acctAddr.acct_phys_zip2, acctAddr.cust_addr_skey, acctAddr.business, acctAddr.sonic_id, acctAddr.billing, acctAddr.mdm_addr_id, acctAddr.edge_id, acctAddr.mdm_addr_src_sys, acctAddr.mdm_cust_skey, acctAddr.mdm_src_addr_type, acctAddr.cass_certified, acctAddr.best_times, acctAddr.siebel_id,acctAddr.appt_required_vod__c,acctAddr.nvs_core_secondary_license__c,acctAddr.maps,sample_status_vod__c,acctAddr.sample_send_status_vod__c,acctAddr.nvs_core_novartis_unique_id__c  FROM  ALL_ALL_B_USA_CRMODS.ODS_ACCT_ADDR acctAddr left anti join final_data fd  on acctAddr.mdm_addr_id=fd.mdm_addr_id and acctAddr.sf_id=fd.sf_id and acctAddr.acct_addr_odw_id = fd.acct_addr_odw_id)""")




spark.sql("truncate table all_all_b_usa_crmods.ods_acct_addr");

spark.sql("insert into all_all_b_usa_crmods.ods_acct_addr select distinct * from final_data");









