--- BACKUP ---

spark.sql("create table ALL_ALL_R_ALL_BKP.ods_prod_xref_bkp27sep2022 like ALL_ALL_B_USA_CRMODS.ods_prod_xref")
spark.sql("insert into ALL_ALL_R_ALL_BKP.ods_prod_xref_bkp27sep2022 select * from ALL_ALL_B_USA_CRMODS.ods_prod_xref")

spark.sql("""create table if not exists ALL_ALL_R_ALL_BKP.ods_prod_xref_t like ALL_ALL_B_USA_CRMODS.ods_prod_xref""")
spark.sql("""truncate table all_all_r_all_bkp.ods_prod_xref_t """)


spark.sql("""with req_data as (select *,row_number() over(order by rec_modify_date) as rn from all_all_b_usa_crmods.ods_prod_xref where prod_xref_odw_id is null), max_odw_id as (select max(prod_xref_odw_id) as max_id,'1' as dummy from all_all_b_usa_crmods.ods_prod_xref), final_data as (select thrptc_area,prod_type,rec_modify_date,rec_insert_date,rec_insert_by,inq_prod_nm,environment,odw_prod_nm,odw_prod_code,sf_id,sf_prod_nm,(rn + b.max_id) prod_xref_odw_id,status,medical_unit,disease_area,siebel_id,brand_code,batch_id_insert,rec_modify_by,batch_id_update from req_data a,max_odw_id b where a.sf_id<>b.dummy )

insert into ALL_ALL_R_ALL_BKP.ods_prod_xref_t select * from final_data""")

spark.sql("""insert into ALL_ALL_R_ALL_BKP.ods_prod_xref_t select ods.* from all_all_b_usa_crmods.ods_prod_xref ods where prod_xref_odw_id is not null""")

spark.sql(""" truncate table ALL_ALL_B_USA_CRMODS.ods_prod_xref """)

spark.sql(""" insert into ALL_ALL_B_USA_CRMODS.ods_prod_xref select  distinct * from all_all_r_all_bkp.ods_prod_xref_t """) 
