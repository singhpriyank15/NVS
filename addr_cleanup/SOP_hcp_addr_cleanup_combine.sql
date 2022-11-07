--BACKUP

spark.sql("create table if not exists all_all_r_all_bkp.ods_contacts_addr_14sep2022 like ALL_ALL_B_USA_CRMODS.ods_contacts_addr")
spark.sql("insert into all_all_r_all_bkp.ods_contacts_addr_14sep2022 select * from ALL_ALL_B_USA_CRMODS.ods_contacts_addr")

-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_contacts_addr_t like ALL_ALL_B_USA_CRMODS.ods_contacts_addr")
spark.sql("truncate table all_all_r_all_bkp.ods_contacts_addr_t")

spark.sql("""with con_dup as (select *,row_number() over(partition by sf_id,contacts_addr_odw_id,mdm_Addr_id,contacts_odw_id order by rec_modify_date desc) as rn from all_all_b_usa_crmods.ods_contacts_addr),req_con as (select mobile_id,ods.email4,ods.email3,ods.email2,ods.email1,ods.contact_fax_2,ods.contact_fax_1,ods.city_post,ods.city,ods.addr_line_2,ods.addr_line_1,ods.src,ods.shipping,ods.mig_stat,ods.mailing,ods.include_in_terr_asgnmt,ods.appt_reqd,ods.cc_flag,ods.business,ods.billing,ods.account,ods.home,ods.contact_name,ods.edge_id,ods.par_edge_id,ods.sonic_id,ods.cam_key,ods.acct_id,ods.cont_id,ods.primary,ods.physician_key,ods.sf_owner_id,ods.sf_is_deleted,ods.sf_id,ods.addr_per_id,ods.contacts_odw_id,ods.contacts_addr_odw_id,ods.mdm_valid_addr_ind,ods.best_times,ods.cass_certified,ods.mdm_addr_id,ods.dly_scrub_date,ods.mth_scrub_date,ods.addr_post_line_2,ods.addr_post_line_1,ods.fax_2,ods.fax_1,ods.batch_id_insert,ods.rec_insert_by,ods.rec_modify_date,ods.rec_insert_date,ods.inactive,ods.staff_notes,ods.office_notes,ods.lic_valid_to_sample,ods.sample_stat,ods.dea_auth_stat_date,ods.dea_auth_stat,ods.dea_sched_cls,ods.dea_lic_addr,ods.dea_expirn_date,ods.dea,ods.zip_post,ods.zip_4_post,ods.zip_4,ods.zip,ods.cntry_code,ods.state,ods.phone_3,ods.phone_2,ods.phone_1,ods.scrub_req,ods.cnty,ods.siebel_id,ods.batch_id_update,ods.rec_modify_by,ods.sample_eligibility_flag,ods.state_distributor_license_exempt_vod__c,ods.nvs_core_secondary_license__c,ods.maps,ods.sample_send_status_vod__c,ods.nvs_core_novartis_unique_id__c from con_dup ods where rn =1)

insert into all_all_r_all_bkp.ods_contacts_addr_t select * from req_con""")


spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_contacts_addr")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_contacts_addr select * from into all_all_r_all_bkp.ods_contacts_addr_t")

