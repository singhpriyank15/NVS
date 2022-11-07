
--BACKUP

spark.sql("create table if not exists all_all_r_all_bkp.ods_site_addr_14sep2022 like ALL_ALL_B_USA_CRMODS.ods_site_addr")
spark.sql("insert into all_all_r_all_bkp.ods_site_addr_14sep2022 select * from ALL_ALL_B_USA_CRMODS.ods_site_addr")

-- Temp Table
spark.sql("create table if not exists all_all_r_all_bkp.ods_site_addr_t like ALL_ALL_B_USA_CRMODS.ods_site_addr")
spark.sql("truncate table all_all_r_all_bkp.ods_site_addr_t")


-- remove duplicate addr_odw_id
spark.sql(""" with dup_odw as ( select site_addr_odw_id,count(1) from ALL_ALL_B_USA_CRMODS.ods_site_addr  where site_addr_odw_id is not null group by site_addr_odw_id having count(1) > 1), veeva_record as (select ods.* from ALL_ALL_B_USA_CRMODS.ods_site_addr ods join dup_odw d on ods.site_addr_odw_id=d.site_addr_odw_id join ph_com_p_usa_veeva.address_vod__c addr on concat('SA',cast(ods.site_addr_odw_id as string)) = addr.address_odw__c join ph_com_p_usa_veeva.account acct  on acct.id = addr.account_vod__c  and concat('S',cast(ods.site_odw_id as string)) = acct.account_odw__c where acct.ispersonaccount = false and acct.isdeleted=false and acct.recordtypeid='0121O000001CG7KQAW' and addr.isdeleted =false)

insert into all_all_r_all_bkp.ods_site_addr_t select * from veeva_record""")

spark.sql("""insert into all_all_r_all_bkp.ods_site_addr_t select ods.* from ALL_ALL_B_USA_CRMODS.ods_site_addr ods left anti join all_all_r_all_bkp.ods_site_addr_t v on ods.site_addr_odw_id=v.site_addr_odw_id""")



spark.sql("truncate table ALL_ALL_B_USA_CRMODS.ods_site_addr")

spark.sql("insert into ALL_ALL_B_USA_CRMODS.ods_site_addr select * from into all_all_r_all_bkp.ods_site_addr_t")


-- validation queries

spark.sql("""select site_addr_odw_id,count(1) from ALL_ALL_B_USA_CRMODS.ods_site_addr  where site_addr_odw_id is not null group by site_addr_odw_id having count(1) > 1""").show(false)