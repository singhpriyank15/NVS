-- delete inactive records in ODS

-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_contacts_addr_t like ALL_ALL_B_USA_CRMODS.ods_contacts_addr")

--BACKUP

spark.sql("create table if not exists all_all_r_all_bkp.ods_contacts_addr_bkp13AUG22 like ALL_ALL_B_USA_CRMODS.ods_contacts_addr")
spark.sql("insert into all_all_r_all_bkp.ods_contacts_addr_bkp13AUG22 select * from ALL_ALL_B_USA_CRMODS.ods_contacts_addr")


spark.sql("""insert into ALL_ALL_B_USA_CRMODS.ods_contacts_addr_H select mdm_valid_addr_ind,best_times,cass_certified,mdm_addr_id,dly_scrub_date,mth_scrub_date,current_timestamp,rec_insert_date,addr_post_line_2,addr_post_line_1,fax_2,fax_1,rec_modify_date,rec_insert_date,batch_id_insert,rec_insert_by,inactive,staff_notes,office_notes,lic_valid_to_sample,sample_stat,dea_auth_stat_date,dea_auth_stat,dea_sched_cls,dea_lic_addr,dea_expirn_date,dea,zip_post,zip_4_post,zip_4,zip,cntry_code,state,phone_3,phone_2,phone_1,mobile_id,email4,email3,email2,email1,contact_fax_2,contact_fax_1,city_post,city,addr_line_2,addr_line_1,src,shipping,mig_stat,mailing,include_in_terr_asgnmt,appt_reqd,cc_flag,business,billing,account,home,contact_name,edge_id,par_edge_id,sonic_id,cam_key,acct_id,cont_id,primary,physician_key,sf_owner_id,sf_is_deleted,sf_id,addr_per_id,contacts_odw_id,contacts_addr_odw_id,cnty,siebel_id,scrub_req,batch_id_update,rec_modify_by,sample_eligibility_flag,State_Distributor_License_Exempt_vod__c,nvs_core_secondary_license__c, maps, sample_send_status_vod__c, nvs_core_novartis_unique_id__c from (SELECT odscont.mobile_id, odscont.email4, odscont.email3, odscont.email2, odscont.email1, odscont.contact_fax_2, odscont.contact_fax_1, odscont.city_post, odscont.city, odscont.addr_line_2, odscont.addr_line_1, odscont.src, odscont.shipping, odscont.mig_stat, odscont.mailing, odscont.include_in_terr_asgnmt, odscont.appt_reqd, odscont.cc_flag, odscont.business, odscont.billing, odscont.account, odscont.home, odscont.contact_name, odscont.edge_id, odscont.par_edge_id, odscont.sonic_id, odscont.cam_key, odscont.acct_id, odscont.cont_id, odscont.primary, odscont.physician_key, odscont.sf_owner_id, odscont.sf_is_deleted, odscont.sf_id, odscont.addr_per_id, odscont.contacts_odw_id, odscont.contacts_addr_odw_id, odscont.mdm_valid_addr_ind, odscont.best_times, odscont.cass_certified, odscont.mdm_addr_id, odscont.dly_scrub_date, odscont.mth_scrub_date, odscont.addr_post_line_2, odscont.addr_post_line_1, odscont.fax_2, odscont.fax_1, odscont.batch_id_insert, odscont.rec_insert_by, current_timestamp rec_modify_date, odscont.rec_insert_date, odscont.inactive, odscont.staff_notes, odscont.office_notes, odscont.lic_valid_to_sample, odscont.sample_stat, odscont.dea_auth_stat_date, odscont.dea_auth_stat, odscont.dea_sched_cls, odscont.dea_lic_addr, odscont.dea_expirn_date, odscont.dea, odscont.zip_post, odscont.zip_4_post, odscont.zip_4, odscont.zip, odscont.cntry_code, odscont.state, odscont.phone_3, odscont.phone_2, odscont.phone_1, odscont.scrub_req, odscont.cnty, odscont.siebel_id, odscont.batch_id_update batch_id_update, odscont.rec_modify_by rec_modify_by, odscont.sample_eligibility_flag,odscont.State_Distributor_License_Exempt_vod__c, odscont.nvs_core_secondary_license__c, odscont.maps, odscont.sample_send_status_vod__c, odscont.nvs_core_novartis_unique_id__c FROM ALL_ALL_B_USA_CRMODS.ods_contacts_addr odscont where upper(odscont.inactive)='TRUE' and odscont.sf_id is  null and odscont.mdm_Addr_id is not null)""")
 
spark.sql("truncate table all_all_r_all_bkp.ods_contacts_addr_t")

spark.sql("insert into all_all_r_all_bkp.ods_contacts_addr_t select ods.* from ALL_ALL_B_USA_CRMODS.ods_contacts_addr ods left anti join (select mdm_Addr_id,contacts_addr_odw_id,contacts_odw_id from ALL_ALL_B_USA_CRMODS.ods_contacts_addr where upper(inactive)='TRUE' and sf_id is  null and mdm_Addr_id is not null) ods1 on ods.mdm_Addr_id=ods1.mdm_Addr_id and ods.contacts_addr_odw_id=ods1.contacts_addr_odw_id and ods.contacts_odw_id=ods1.contacts_odw_id")

spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_contacts_addr")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_contacts_addr select * from into all_all_r_all_bkp.ods_contacts_addr_t")




