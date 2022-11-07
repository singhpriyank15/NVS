--sf_id duplicate removal based on latest odw_id

--- BACKUP ---

spark.sql("create table ALL_ALL_R_ALL_BKP.ods_tm_off_terr_BKP_3SEP22 like ALL_ALL_B_USA_CRMODS.ods_tm_off_terr")
spark.sql("insert into ALL_ALL_R_ALL_BKP.ods_tm_off_terr_BKP_3SEP22 select * from ALL_ALL_B_USA_CRMODS.ods_tm_off_terr")

--Temp Table --
spark.sql("create table if not exists ALL_ALL_R_ALL_BKP.ods_tm_off_terr_t like ALL_ALL_B_USA_CRMODS.ods_tm_off_terr")

spark.sql("""with dup_rec as (select *,row_number() over(partition by sf_id order by odw_id desc) as rnk from ALL_ALL_B_USA_CRMODS.ods_tm_off_terr), final_data as ( select terr_odw_id,emp_id,is_deleted,duration,src,time,terr,stat,last_modified_date,last_activity_date,create_date,tm_off_hrs,tm_off_terr_date,reason,tm_off_terr_nm,rep_id,odw_id,owner_id,sf_id,edge_id,batch_id_insert,batch_id_update,rec_insert_by,rec_insert_date,rec_modify_by,rec_modify_date from dup_rec where rnk =1)

insert into table singhp2j.ods_tm_off_terr_t select * from final_data""")

spark.sql("""truncate table ALL_ALL_B_USA_CRMODS.ods_tm_off_terr""")

spark.sql("""insert into ALL_ALL_B_USA_CRMODS.ods_tm_off_terr select * from ALL_ALL_R_ALL_BKP.ods_tm_off_terr_t""")


-- Validation 


select sf_id,count(*) from ALL_ALL_B_USA_CRMODS.ods_tm_off_terr where upper(is_deleted) = 'FALSE' group by sf_id having count(*) > 1

spark.sql("select sf_id from ALL_ALL_B_USA_CRMODS.ods_tm_off_terr minus select sf_id from singhp2j.ods_tm_off_terr_t").show(false)




























