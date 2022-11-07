--- BACKUP ---

spark.sql("create table ALL_ALL_R_ALL_BKP.ods_medpro_log_bkp30sep22 like ALL_ALL_B_USA_CRMODS.ods_medpro_log")
spark.sql("insert into ALL_ALL_R_ALL_BKP.ods_medpro_log_bkp30sep22 select * from ALL_ALL_B_USA_CRMODS.ods_medpro_log")

spark.sql("create table if not exists ALL_ALL_R_ALL_BKP.ods_medpro_log_t like ALL_ALL_B_USA_CRMODS.ods_medpro_log")
spark.sql("truncate table ALL_ALL_R_ALL_BKP.ods_medpro_log_t")



-- remove duplicate  on basis of dsr_sf_id,odw_id

spark.sql("""with dup_rec as (select dsr_sf_id,odw_id,dsr_request_id,count(1) from all_all_b_usa_crmods.ods_medpro_log group by dsr_request_id,odw_id,dsr_sf_id having count(1) > 1), lnd_data as (select pai_number,CASE WHEN TRIM(upper(lnd.pai_status)) = 'ACCEPTED' THEN 'COMPLETED' ELSE lnd.pai_status END status from ALL_ALL_R_GBL_MDM.LND_VNTG_MEDPRO_PAI lnd join dup_rec d on trim(lnd.pai_number)=trim(d.dsr_request_id))

insert into ALL_ALL_R_ALL_BKP.ods_medpro_log_t select ods.* from all_all_b_usa_crmods.ods_medpro_log ods join lnd_data lnd on trim(ods.dsr_request_id)=trim(lnd.pai_number) and upper(lnd.status)=upper(ods.status)""")


spark.sql("insert into ALL_ALL_R_ALL_BKP.ods_medpro_log_t select ods.* from all_all_b_usa_crmods.ods_medpro_log ods left anti join ALL_ALL_R_ALL_BKP.ods_medpro_log_t tmp on ods.dsr_request_id=tmp.dsr_request_id and ods.dsr_sf_id=tmp.dsr_sf_id and ods.odw_id=tmp.odw_id")

spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_medpro_log")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_medpro_log select * from ALL_ALL_R_ALL_BKP.ods_medpro_log_t ")


--removing duplicate on basis of batch_id_update

spark.sql("truncate table ALL_ALL_R_ALL_BKP.ods_medpro_log_t")

spark.sql("""with distinct_rec as (select *,row_number() over(partition by dsr_sf_id,odw_id order by batch_id_update desc) as rn from ALL_ALL_B_USA_CRMODS.ods_medpro_log),req_data as (select odw_id,record_type_id,dsr_sf_id,dsr_request_id,cust_odw_id,cust_addr_odw_id,cust_stt_odw_id,unique_id,out_lname,out_fname,out_mid_nm,out_salutation,out_suffix,out_addr_line_1,out_addr_line_2,out_city,out_state,out_zip_code,out_zip_code_4,out_lic,out_lic_state,out_dea,out_mdm_party_id,out_terr_id,out_field_force,out_medpro_id,out_script_track_no,out_med_edu_no,out_cntry_code,out_phone_no,out_ims_degree,out_nov_spclty_code,out_npi_no,out_nov_id,out_rep_spclty_code,out_degree,status,impact_flag,in_record_type,in_record_seq,in_lname,in_fname,in_mid_nm,in_salutation,in_suffix,in_addr_line_1,in_addr_line_2,in_city,in_state,in_zip_code,in_zip_code_4,in_lic,in_lic_state,in_dea,in_mdm_party_id,in_terr_id,in_field_force,in_medpro_id,in_script_tracking_no,in_med_edu_no,in_cntry_code,in_phone_no,in_ims_degree,in_nov_spclty_code,in_npi_no,in_cust_comment_fld,in_cust_comment_fld_1,in_cust_comment_fld_2,in_cust_comment_fld_3,in_medpro_id_match_method,in_sln_medpro_id,in_licensure_state,in_lic_1,in_lic_issue_date,in_lic_exp_date,in_lic_status,in_sample_overall_flag,in_sample_lic_status,in_sample_exp_overall_flag,in_state_grace_period,in_cust_state_grace_period,in_adj_state_lic_exp_date,in_sample_degree_status,in_sample_fed_sanction,in_sample_last_rcvd,in_addr_verified,in_addr_adjud_code,in_addr_adjud_desc,in_addr_comment_fld,in_spclty_verified,in_spclty_adjud_code,in_spclty_adjud_desc,in_spclty_comment_fld,in_dea_medpro_id,in_dea_1,in_dea_activity_code,in_dea_exp_date,in_dea_status,in_dea_addr_line_1,in_dea_addr_line_2,in_dea_addr_line_3,in_dea_city,in_dea_state,in_dea_zip_code,in_dea_zip_code_4,in_ama_medpro_id,in_ama_me_no,in_ama_degree,in_ama_prim_spclty,in_ama_sec_spclty,in_ama_prim_spclty_acronym,in_ama_sec_spclty_acronym,in_ama_mpa,in_ama_practice_type,in_ama_birth_date,in_ama_birth_city,in_ama_birth_cntry,in_ama_gender,in_ama_graduation_yr,in_ama_graduation_school,in_ama_pdrp_flag,in_npi_medpro_id,in_npi_num_1,in_npi_state_lic,in_npi_state_lic_no,in_npi_prim_txnmy_code,in_npi_prim_txnmy_nm,in_best_visit_addr_1,in_best_visit_addr_2,in_best_visit_addr_3,in_best_visit_city,in_best_visit_state,in_best_visit_zip_code,in_best_visit_zip_code_4,in_best_prim_spclty,in_best_sec_spclty,in_reserved_fld_1,in_reserved_fld_2,in_reserved_fld_3,in_reserved_fld_4,in_reserved_fld_5,in_reserved_fld_6,in_reserved_fld_7,in_decided_spclty,in_disposition_date,mdm_reject_flag,mdm_reject_reason,batch_id_insert,batch_id_update,rec_insert_by,rec_insert_date,rec_modify_by,rec_modify_date from distinct_rec where rn=1)

insert into ALL_ALL_R_ALL_BKP.ods_medpro_log_t select * from req_data""")


spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_medpro_log")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_medpro_log select * from ALL_ALL_R_ALL_BKP.ods_medpro_log_t ")




