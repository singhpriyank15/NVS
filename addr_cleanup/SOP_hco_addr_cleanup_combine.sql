--BACKUP

spark.sql("create table if not exists all_all_r_all_bkp.ods_site_addr_14sep2022 like ALL_ALL_B_USA_CRMODS.ods_site_addr")
spark.sql("insert into all_all_r_all_bkp.ods_site_addr_14sep2022 select * from ALL_ALL_B_USA_CRMODS.ods_site_addr")

-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_site_addr_t like ALL_ALL_B_USA_CRMODS.ods_site_addr")
spark.sql("truncate table all_all_r_all_bkp.ods_site_addr_t")

spark.sql("""with site_dup as (select *,row_number() over(partition by sf_id,site_addr_odw_id,mdm_Addr_id,site_odw_id order by rec_modify_date desc) as rn from all_all_b_usa_crmods.ods_site_addr),req_site as (select siebel_id,site_addr_odw_id,sf_id,sf_owner_id,sf_is_deleted,site_key,site_odw_id,dea,dea_expirn_date,dea_lic_addr,site_name,home,mailing,shipping,mobile_id,phone,phone_2,fax,fax_2,site_phys_addr_line1,site_phys_addr_line2,site_phys_addr_line3,site_phys_city,site_phys_country,site_phys_county,site_phys_state,site_phys_zip,site_post_addr_line1,site_post_addr_line2,site_post_addr_line3,site_post_city,site_post_country,site_post_county,site_post_state,site_post_zip1,site_post_zip2,mdm_valid_addr_ind,zip_4,lic_expirn_date,lic_stat,lic_valid_to_sample,lic,staff_notes,src,office_notes,inc_in_terr_asgnmt,appt_reqd,billing,business,primary,sample_stat,rec_insert_by,rec_insert_date,rec_modify_date,batch_id_insert,inactive,is_deleted,mdm_addr_id,edge_id,mdm_addr_src_sys,mdm_cust_skey,mdm_src_addr_type,cust_addr_skey,cass_certified,best_times,rec_modify_by,batch_id_update,nvs_core_secondary_license__c,maps,sample_send_status_vod__c,nvs_core_novartis_unique_id__c from site_dup where rn =1)

insert into all_all_r_all_bkp.ods_site_addr_t select * from req_site""")






spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_site_addr")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_site_addr select * from into all_all_r_all_bkp.ods_site_addr_t")


