--- BACKUP ---

spark.sql("create table ALL_ALL_R_ALL_BKP.ods_vst_smp_bkp30sep22 like ALL_ALL_B_USA_CRMODS.ods_vst_smp")
spark.sql("insert into ALL_ALL_R_ALL_BKP.ods_vst_smp_bkp30sep22 select * from ALL_ALL_B_USA_CRMODS.ods_vst_smp")

spark.sql("create table if not exists ALL_ALL_R_ALL_BKP.ods_vst_smp_t like ALL_ALL_B_USA_CRMODS.ods_vst_smp")
spark.sql("truncate table ALL_ALL_R_ALL_BKP.ods_vst_smp_t")


-- remove the duplicate for the column prod


spark.sql("""with dup_rec as (select sf_id,vst_smp_odw_id,count(*) from all_all_b_usa_crmods.ods_vst_smp where sf_id is not null group by vst_smp_odw_id,sf_id having count(*) > 1)

insert into ALL_ALL_R_ALL_BKP.ods_vst_smp_t select ods.* from ALL_ALL_B_USA_CRMODS.ods_vst_smp ods left anti join dup_rec d on ods.sf_id=d.sf_id""")


spark.sql("""insert INTO ALL_ALL_B_USA_CRMODS.ODS_VISIT_SAMPLE_H SELECT a.prod_type prod_type, a.prod_sf_id prod_sf_id, a.odw_cust_type odw_cust_type, a.src src, a.batch_id_insert batch_id_insert, a.rec_insert_date rec_insert_date, a.rec_modify_date rec_modify_date, current_timestamp() eff_end_date , a.rec_insert_by rec_insert_by, nvl(a.rec_modify_date,a.rec_insert_date) eff_start_date, a.vst_odw_id visit_odw_id, a.mob_id mob_id, a.create_date create_date, a.qty qty, a.prod product, a.lot_no lot_no, a.is_par_call is_par_call, a.vst_smp_date vst_samp_date, a.vst_smp_call vst_samp_call, a.cust_odw_id cust_odw_id, a.vst_smp_nm vis_samp_nm, a.sf_id sf_id, a.vst_smp_odw_id vis_samp_odw_id, a.is_deleted is_deleted, a.batch_id_update batch_id_update, a.rec_modify_by rec_modify_by from ALL_ALL_B_USA_CRMODS.ods_vst_smp a left anti join ALL_ALL_R_ALL_BKP.ods_vst_smp_t d on a.sf_id=d.sf_id""")



spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_vst_smp")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_vst_smp select * from ALL_ALL_R_ALL_BKP.ods_vst_smp_t ")



