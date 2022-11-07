--- BACKUP ---

spark.sql("create table ALL_ALL_R_ALL_BKP.ods_vst_smp_bkp30sep22_2 like ALL_ALL_B_USA_CRMODS.ods_vst_smp")
spark.sql("insert into ALL_ALL_R_ALL_BKP.ods_vst_smp_bkp30sep22_2 select * from ALL_ALL_B_USA_CRMODS.ods_vst_smp")

spark.sql("create table if not exists ALL_ALL_R_ALL_BKP.ods_vst_smp_t like ALL_ALL_B_USA_CRMODS.ods_vst_smp")
spark.sql("truncate table ALL_ALL_R_ALL_BKP.ods_vst_smp_t")


-- remove the duplicate for the column prod

spark.sql("""with smp_tbl as (select *,row_number() over(partition by sf_id order by vst_smp_odw_id desc,rec_modify_date desc) as rn from all_all_b_usa_crmods.ods_vst_smp where sf_id is not null), distinct_record as ( select * from smp_tbl where rn=1)

insert into ALL_ALL_R_ALL_BKP.ods_vst_smp_t select ods.* from ALL_ALL_B_USA_CRMODS.ods_vst_smp ods join distinct_record d on d.sf_id=ods.sf_id and d.vst_smp_odw_id=ods.vst_smp_odw_id""")

spark.sql("insert into ALL_ALL_R_ALL_BKP.ods_vst_smp_t select * from ALL_ALL_B_USA_CRMODS.ods_vst_smp where sf_id is null")

spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_vst_smp")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_vst_smp select * from ALL_ALL_R_ALL_BKP.ods_vst_smp_t ")

