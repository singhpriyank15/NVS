spark.sparkContext.setLogLevel("ERROR")

spark.sql("""create table all_all_r_all_bkp.ods_site_addr_29thaug22_deletes like all_all_b_usa_crmods.ods_site_addr""")


spark.sql("""insert into all_all_r_all_bkp.ods_site_addr_29thaug22_deletes select * from all_all_b_usa_crmods.ods_site_addr""")

spark.sql("""with union_data as (select * from all_all_r_all_bkp.hco_addr_base_query_case1 union select * from all_all_r_all_bkp.hco_addr_base_query_case2), addr_dup as (select address_mdm_id__c,count(1) from union_data group by address_mdm_id__c having count(1) =1), case1a as (select id,address_mdm_id__c,address_odw__c,account_odw__c from all_all_r_all_bkp.hco_addr_base_query_case1 where sf_id is null and addr_o_addr_odw_id is null and addr_sf_sf_id is null), req_data as ( select u.* from union_data u left anti join case1a c on u.id=c.id and u.address_odw__c=c.address_odw__c  and u.account_odw__c=c.account_odw__c)

select ods.* from all_all_b_usa_crmods.ods_site_addr ods left anti join req_data r on r.id = ods.sf_id or r.address_mdm_id__c=ods.mdm_addr_id or r.address_odw__c=trim(concat('SA',cast(ods.site_addr_odw_id as string)))""").createOrReplaceTempView("final_data")

-- Delete from site_addr
spark.sql("""INSERT INTO all_all_b_usa_crmods.ods_site_addr_h SELECT best_times, cass_certified, cust_addr_skey, mdm_src_addr_type, mdm_cust_skey, mdm_addr_src_sys, edge_id, mdm_addr_id, is_deleted, inactive, batch_id_insert,current_timestamp eff_end_date, NVL(rec_insert_date,rec_modify_date)eff_start_date, rec_insert_by, sample_stat, primary, business, billing, appt_reqd, inc_in_terr_asgnmt, office_notes, src, staff_notes, lic, lic_valid_to_sample, lic_stat, lic_expirn_date, zip_4, mdm_valid_addr_ind, site_post_zip2, site_post_zip1, site_post_state, site_post_county, site_post_country, site_post_city, site_post_addr_line3, site_post_addr_line2, site_post_addr_line1, site_phys_zip, site_phys_state, site_phys_county, site_phys_country, site_phys_city, site_phys_addr_line3, site_phys_addr_line2, site_phys_addr_line1, fax_2, fax, phone_2, phone, mobile_id, shipping, mailing, home, site_name, dea_lic_addr, dea_expirn_date, dea, site_odw_id, site_key, sf_is_deleted, sf_owner_id, sf_id, site_addr_odw_id, siebel_id, rec_modify_date,current_timestamp rec_insert_date, batch_id_update, rec_modify_by,nvs_core_secondary_license__c, maps, sample_send_status_vod__c, nvs_core_novartis_unique_id__c    FROM all_all_b_usa_crmods.ods_site_addr ods left anti join final_data fd on ods.mdm_addr_id=fd.mdm_addr_id and ods.sf_id=fd.sf_id and ods.site_addr_odw_id = fd.site_addr_odw_id""")


spark.sql("truncate table all_all_b_usa_crmods.ods_site_addr");

spark.sql("insert into all_all_b_usa_crmods.ods_site_addr select distinct * from final_data");








