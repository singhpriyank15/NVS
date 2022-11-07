spark.sql(""" create table all_all_r_all_bkp.ods_site_30sep22 like all_all_b_usa_crmods.ods_site """)
spark.sql(""" insert into all_all_r_all_bkp.ods_site_30sep22 select * from  all_all_b_usa_crmods.ods_site """)

spark.sql(""" select count(*) from all_all_b_usa_crmods.ods_site """).show
spark.sql(""" select count(*) from all_all_r_all_bkp.ods_site_30sep22 """).show

spark.sql(""" select distinct * from all_all_b_usa_crmods.ods_site """).count

--qa--9415699--prod--9927198

spark.sql(""" create table all_all_r_all_bkp.demo_h select odssite.mdm_party_id,odssite.sf_id,customer_id,mdm_status_code from all_all_b_usa_crmods.ods_site odssite left anti join (select nvs_core_novartis_unique_id__c,account_odw__c from ph_com_p_usa_veeva.account where ispersonaccount = false and isdeleted = false and recordtypeid = '0121O000001CG7KQAW')
ac on trim(nvl(odssite.mdm_party_id,'@'))=trim(nvl(ac.nvs_core_novartis_unique_id__c,'@')) or concat('S',cast(odssite.site_odw_id as string))=ac.account_odw__c join (select customer_id,mdm_status_code from all_all_e_gbl_customer.idl_mdm_account) i on trim(i.customer_id)=trim(odssite.mdm_party_id) where odssite.sf_id is null and upper(trim(i.mdm_status_code))='I' """)

spark.sql(""" select count(*) from all_all_r_all_bkp.demo_h """).show

spark.sql(""" insert into all_all_b_usa_crmods.ods_site_h SELECT  odssite.academic_community,odssite.pc_kam_key_account,odssite.request_code,odssite.cust_key,odssite.mdm_src_id,odssite.mdm_cust_subtype,odssite.mdm_cust_type,odssite.mm_kam_acct_tier,odssite.exec_sum,odssite.fully_insured_rx_lives,odssite.comm_insured_rx_lives,odssite.comm_self_insured_rx_lives,odssite.med_caid,odssite.med_care,odssite.unos_id,odssite.alias_name,odssite.group_s,odssite.teach_hospital_flag,odssite.territories,odssite.totl_bed_cnt,odssite.totl_lives,odssite.totl_mds_dos,odssite.totl_orgc_surg,odssite.totl_phrmts,odssite.totl_phys_enrolled,odssite.totl_revenue,odssite.tumor_types,odssite.veterans_affairs_hospital,odssite.visn,odssite.ytd_calls,odssite.ytd_stt_spend,odssite.inactive,odssite.rec_insert_by,nvl(odssite.rec_modify_date,odssite.rec_insert_date) eff_start_date,current_timestamp eff_end_date,odssite.batch_id_insert,odssite.centre_no,odssite.sonic_id,odssite.is_deleted,odssite.dea,odssite.phone,odssite.email,odssite.dept_no,odssite.totl_site,odssite.sonic_org_ext_row_id,odssite.site_cmi,odssite.site_cot_specialty,odssite.site_resident_program_flag,odssite.site_resident_count,odssite.site_mpn,odssite.site_mpn_order,odssite.site_physician_key,odssite.site_main_location_flag,odssite.mdm_party_id,odssite.edge_id,odssite.owner_dept,odssite.usmm_kam,odssite.tot_rx_lives,odssite.pbm_crv_out,odssite.spclty_phrmcy,odssite.case_mgr,odssite.num_us_emp,odssite.ebc,odssite.chn_fclty_phrmcy_prvdr,odssite.tot_fclts,odssite.med_d_beds,odssite.parta_beds,odssite.phrmcy_type,odssite.lvl,odssite.comm_cntrct_date,odssite.comm_cntrct_stat,odssite.medcr_partd_cntrt_date,odssite.medcr_partd_cntrt_stat,odssite.medcd_partd_cntrt_date,odssite.medcd_partd_cntrt_stat,odssite.tot_covd_lives,odssite.self_insurd,odssite.ful_insurd,odssite.trust,odssite.top5_geo,odssite.comm_drug_trnd_prcnt,odssite.spclty_pmpm,odssite.spclty_drug_cntr_prcnt,odssite.tot_sls,odssite.spclty_tot_prcnt_sls,odssite.rx_sls,odssite.tot_nvs_sls,odssite.nvs_rtns,odssite.nvs_rtns_prcnt,odssite.top5_nvs_prod,odssite.invtry_mnths_on_hnd,odssite.invtry_as_of_date,odssite.ex_dspns_rt_prcnt,odssite.medcr_prcnt,odssite.medcd_prcnt,odssite.oth_thrd_prty_prcnt,odssite.cthmnts,odssite.departments,odssite.rsdncy_prgms,odssite.tot_rx_bdgt,odssite.add_degs,odssite.job_ttl,odssite.plntrk_id,odssite.hsg_id,odssite.hlth_ldrs_id,odssite.dea_no,odssite.ftf_parent_mdm_id,odssite.onc_cntrt_date,odssite.onc_cntrt_stat,odssite.pbm_tot_lives,odssite.emplr,odssite.hmo_ppo,odssite.bcbs,odssite.medcr_mm_pbm,odssite.oth_fed,odssite.other,odssite.tot_of_phrm,odssite.cntr_date,odssite.cntrt_stat,odssite.spclty_drug_trnd,odssite.onc_kam,odssite.tot_rev,odssite.net_inc_loss,odssite.medc_loss_ratio,odssite.comm_prem_pmpm,odssite.ftf_prvdr_mdm_id,odssite.mdm_src,odssite.cust_excptn_flag,odssite.site_agmt_revw_with,odssite.pam_key_account,odssite.patnt_intr,odssite.site_agmt_revw_date,odssite.scz_vldt_stat,odssite.payer_id,odssite.sec_scz_vldt_stat,odssite.brd_cert_in,odssite.key_oso_mm_contact_nov,odssite.contacts_identifier,odssite.tobi_validation_status,odssite.hem_msl_contact,odssite.onc_msl_contact,odssite.rd_msl_contact,odssite.description,odssite.cln_rsrch_capable,odssite.heor_rsrch_capable,odssite.cln_rsrch_active,odssite.mangd_market_segmnt,odssite.heor_rsrch_active,odssite.org_strata_level,odssite.center_of_excell,odssite.mangd_market_phy_grp,odssite.created_by_id,odssite.last_modified_id,odssite.create_date,odssite.last_modified_date,odssite.siebel_id,odssite.midb_site_row_id,odssite.addr_row_id,odssite.sonic_create_date,odssite.sonic_org_type,odssite.comments,odssite.site_odw_id,odssite.site_vendor_org_id,odssite.cam_key,odssite.sf_id,odssite.sf_is_deleted,odssite.sf_owner_id,odssite.acct_odw_id,odssite.cam_acct_key,odssite.site_name,odssite.site_dba_name,odssite.formatted_nm,odssite.hin,odssite.ddd_no,odssite.ddd_sub_cat_code,odssite.cust_type,odssite.aha,odssite.birth_date,odssite.bus,odssite.bus_desc,odssite.contracts_prcs,odssite.credentials,odssite.do_not_sync_sales_data,odssite.employees,odssite.excld_from_terr_asgnmt_rules,odssite.excld_from_zip_to_terr_prcsg,odssite.fax,odssite.fax_2,odssite.fellowship_location,odssite.include_in_terr_asgnmt,odssite.may_edit,odssite.me,odssite.modl,odssite.net_income_loss,odssite.npi,odssite.oth_rel_info,odssite.ownership,odssite.pager,odssite.phm_24_hr_flag,odssite.phm_disp_cls_code,odssite.phm_disp_type_code,odssite.phm_store_no,odssite.site_phn_1,odssite.site_phn_2,odssite.programs,odssite.regional_strategy,odssite.site_census_beds,odssite.site_cot_class,odssite.site_cot_facility_type,odssite.site_fips_county,odssite.site_fips_state,odssite.site_license_beds,odssite.site_msa,odssite.site_or_surgeries,odssite.site_procedures,odssite.site_profit_status,odssite.site_segment,odssite.site_staffed_beds,odssite.site_status_indicator,odssite.site_surgeries,odssite.site_web_site_url,odssite.spend_cap,odssite.src,odssite.staff_notes,odssite.tax_stat,odssite.cpf_elgbl,odssite.ms_kam_acct_tier,odssite.ff_department,odssite.lcz_attstn,odssite.lcz_attst_prod,odssite.xolair_attst_prod,odssite.prod_attstn,odssite.compliance_attstn,odssite.stt_and_fed_law_attstn,odssite.future_coe__c,odssite.derm_rheum_clinic__c,odssite.msl_site_name,odssite.attst_prod,odssite.portfolio_msl_contact,odssite.rec_modify_date,odssite.rec_insert_date,odssite.rec_modify_by,odssite.batch_id_update,odssite. nvs_core_profit_status__c,odssite.beds__c,odssite.country_vod__c,odssite.external_id_vod__c,odssite.nvs_core_account_category__c,odssite.nvs_core_account_subtype__c,odssite.nvs_core_credentialing_required__c,odssite.nvs_core_credentialing_service_company__c,odssite.nvs_core_digital_id__c,odssite.nvs_core_emr_system__c,odssite.nvs_core_fiscal_id__c,odssite.nvs_core_key_account__c,odssite.nvs_core_legal_name__c,odssite.nvs_core_national_code__c,odssite.nvs_core_pricing_program__c,odssite.nvs_core_salesforce_id__c,odssite.nvs_core_sap_id__c,odssite.nvs_core_status__c,odssite.nvs_core_top_account_name__c,odssite.nvs_core_vendor_id__c,odssite.primary_parent_vod__c,odssite.recordtype,odssite.specialty_1_vod__c,odssite.specialty_2_vod__c,odssite.hco_phone  from ALL_ALL_B_USA_CRMODS.ods_site odssite join all_all_r_all_bkp.demo_h d on trim(odssite.mdm_party_id)=trim(d.mdm_party_id) """)

---5 ids

spark.sql(""" create table all_all_r_all_bkp.ods_site_temp11 like all_all_b_usa_crmods.ods_site """)

spark.sql(""" insert into all_all_r_all_bkp.ods_site_temp11 select * from  all_all_b_usa_crmods.ods_site c 
left anti join all_all_r_all_bkp.demo_h d on trim(c.mdm_party_id)=trim(d.mdm_party_id) """)

spark.sql(""" select count(*) from all_all_r_all_bkp.ods_site_temp11 """).show

--9415694

spark.sql(""" select count(*) from all_all_b_usa_crmods.ods_site """).show

--9415699

spark.sql(""" truncate table all_all_b_usa_crmods.ods_site """)

spark.sql(""" insert into all_all_b_usa_crmods.ods_site select distinct * from all_all_r_all_bkp.ods_site_temp11 """) 

spark.sql(""" select count(*) from all_all_b_usa_crmods.ods_site """).show 

spark.sql(""" drop table if exists all_all_r_all_bkp.ods_site_temp11 """) 

spark.sql(""" drop table if exists all_all_r_all_bkp.demo_h """)

----
validation :-

---demo_h count :-

spark.sql(""" select odssite.mdm_party_id,odssite.sf_id,customer_id,mdm_status_code from all_all_b_usa_crmods.ods_site odssite left anti join (select nvs_core_novartis_unique_id__c,account_odw__c from ph_com_p_usa_veeva.account where ispersonaccount = false and isdeleted = false and recordtypeid = '0121O000001CG7KQAW')
ac on trim(nvl(odssite.mdm_party_id,'@'))=trim(nvl(ac.nvs_core_novartis_unique_id__c,'@')) or concat('S',cast(odssite.site_odw_id as string))=ac.account_odw__c join (select customer_id,mdm_status_code from all_all_e_gbl_customer.idl_mdm_account) i on trim(i.customer_id)=trim(odssite.mdm_party_id) where odssite.sf_id is null and upper(trim(i.mdm_status_code))='I' """)


spark.sql(""" select count(*) from all_all_b_usa_crmods.ods_site """).show 
---ods_site count will have removed the records present in demo_h table after sop run---


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
spark.sql(""" create table all_all_r_all_bkp.ods_site_30sep22_bkp1 like all_all_b_usa_crmods.ods_site """)
spark.sql(""" insert into all_all_r_all_bkp.ods_site_30sep22_bkp1 select * from  all_all_b_usa_crmods.ods_site """)

spark.sql(""" select count(*) from all_all_b_usa_crmods.ods_site """).show
spark.sql(""" select count(*) from all_all_r_all_bkp.ods_site_30sep22_bkp1 """).show

spark.sql(""" create table all_all_r_all_bkp.ods_site_t1 like all_all_b_usa_crmods.ods_site """)

spark.sql(""" insert into all_all_r_all_bkp.ods_site_t1 select o.ff_department,o.cpf_elgbl,o.ms_kam_acct_tier,o.payer_id,o.description,o.cln_rsrch_capable,o.heor_rsrch_capable,o.cln_rsrch_active,o.mangd_market_segmnt,o.heor_rsrch_active,o.org_strata_level,o.center_of_excell,o.mangd_market_phy_grp,o.created_by_id,o.last_modified_id,o.create_date,o.last_modified_date,o.siebel_id,o.midb_site_row_id,o.addr_row_id,o.sonic_create_date,o.sonic_org_type,o.comments,o.site_resident_program_flag,o.site_resident_count,o.site_mpn,o.site_mpn_order,o.site_physician_key,o.site_main_location_flag,o.mdm_party_id,o.edge_id,o.owner_dept,o.usmm_kam,o.tot_rx_lives,o.pbm_crv_out,o.spclty_phrmcy,o.case_mgr,o.num_us_emp,o.ebc,o.chn_fclty_phrmcy_prvdr,o.tot_fclts,o.med_d_beds,o.parta_beds,o.phrmcy_type,o.lvl,o.comm_cntrct_date,o.comm_cntrct_stat,o.medcr_partd_cntrt_date,o.medcr_partd_cntrt_stat,o.medcd_partd_cntrt_date,o.medcd_partd_cntrt_stat,o.tot_covd_lives,o.self_insurd,o.ful_insurd,o.trust,o.top5_geo,o.comm_drug_trnd_prcnt,o.spclty_pmpm,o.spclty_drug_cntr_prcnt,o.tot_sls,o.spclty_tot_prcnt_sls,o.rx_sls,o.tot_nvs_sls,o.nvs_rtns,o.nvs_rtns_prcnt,o.top5_nvs_prod,o.invtry_mnths_on_hnd,o.invtry_as_of_date,o.ex_dspns_rt_prcnt,o.medcr_prcnt,o.medcd_prcnt,o.oth_thrd_prty_prcnt,o.cthmnts,o.departments,o.rsdncy_prgms,o.tot_rx_bdgt,o.add_degs,o.job_ttl,o.plntrk_id,o.hsg_id,o.hlth_ldrs_id,o.dea_no,o.ftf_parent_mdm_id,o.onc_cntrt_date,o.onc_cntrt_stat,o.pbm_tot_lives,o.emplr,o.hmo_ppo,o.bcbs,o.medcr_mm_pbm,o.oth_fed,o.other,o.tot_of_phrm,o.cntr_date,o.cntrt_stat,o.spclty_drug_trnd,o.onc_kam,o.tot_rev,o.net_inc_loss,o.medc_loss_ratio,o.comm_prem_pmpm,o.ftf_prvdr_mdm_id,o.mdm_src,o.cust_excptn_flag,o.cust_key,o.mdm_src_id,o.mdm_cust_subtype,o.mdm_cust_type,o.mm_kam_acct_tier,o.exec_sum,o.fully_insured_rx_lives,o.comm_insured_rx_lives,o.comm_self_insured_rx_lives,o.med_caid,o.med_care,o.unos_id,o.alias_name,o.future_coe__c,o.attst_prod,o.msl_site_name,o.group_s,o.portfolio_msl_contact,o.lcz_attstn,o.lcz_attst_prod,o.xolair_attst_prod,o.prod_attstn,o.compliance_attstn,o.stt_and_fed_law_attstn,o.derm_rheum_clinic__c,o.site_odw_id,o.site_vendor_org_id,o.cam_key,o.sf_id,o.sf_is_deleted,o.sf_owner_id,o.acct_odw_id,o.cam_acct_key,o.site_name,o.site_dba_name,o.formatted_nm,o.hin,o.ddd_no,o.ddd_sub_cat_code,o.cust_type,o.aha,o.birth_date,o.bus,o.bus_desc,o.contracts_prcs,o.credentials,o.do_not_sync_sales_data,o.employees,o.excld_from_terr_asgnmt_rules,o.excld_from_zip_to_terr_prcsg,o.fax,o.fax_2,o.fellowship_location,o.include_in_terr_asgnmt,o.may_edit,o.me,o.modl,o.net_income_loss,o.npi,o.oth_rel_info,o.ownership,o.pager,o.phm_24_hr_flag,o.phm_disp_cls_code,o.phm_disp_type_code,o.phm_store_no,o.site_phn_1,o.site_phn_2,o.programs,o.regional_strategy,o.site_census_beds,o.site_cot_class,o.site_cot_facility_type,o.site_fips_county,o.site_fips_state,o.site_license_beds,o.site_msa,o.site_or_surgeries,o.site_procedures,o.site_profit_status,o.site_segment,o.site_staffed_beds,o.site_status_indicator,o.site_surgeries,o.site_web_site_url,o.spend_cap,o.src,o.staff_notes,o.tax_stat,o.teach_hospital_flag,o.territories,o.totl_bed_cnt,o.totl_lives,o.totl_mds_dos,o.totl_orgc_surg,o.totl_phrmts,o.totl_phys_enrolled,o.totl_revenue,o.tumor_types,o.veterans_affairs_hospital,o.visn,o.ytd_calls,o.ytd_stt_spend,o.inactive,o.rec_insert_by,o.rec_insert_date,current_timestamp() rec_modify_date,o.batch_id_insert,o.centre_no,o.sonic_id,o.dea,o.is_deleted,o.sonic_org_ext_row_id,o.phone,o.email,o.dept_no,o.totl_site,o.site_cmi,o.site_cot_specialty,o.hem_msl_contact,o.onc_msl_contact,o.rd_msl_contact,o.academic_community,o.pc_kam_key_account,o.pam_key_account,o.request_code,o.patnt_intr,o.site_agmt_revw_date,o.scz_vldt_stat,o.sec_scz_vldt_stat,o.brd_cert_in,o.key_oso_mm_contact_nov,o.contacts_identifier,o.tobi_validation_status,o.site_agmt_revw_with,'MDM' rec_modify_by,o.batch_id_update,o.nvs_core_profit_status__c,o.beds__c,o.country_vod__c,o.external_id_vod__c,o.nvs_core_account_category__c,o.nvs_core_account_subtype__c,o.nvs_core_credentialing_required__c,o.nvs_core_credentialing_service_company__c,o.nvs_core_digital_id__c,o.nvs_core_emr_system__c,o.nvs_core_fiscal_id__c,o.nvs_core_key_account__c,o.nvs_core_legal_name__c,o.nvs_core_national_code__c,o.nvs_core_pricing_program__c,o.nvs_core_salesforce_id__c,o.nvs_core_sap_id__c,o.nvs_core_status__c,o.nvs_core_top_account_name__c,o.nvs_core_vendor_id__c,o.primary_parent_vod__c,o.recordtype,o.specialty_1_vod__c,o.specialty_2_vod__c,o.hco_phone from all_all_b_usa_crmods.ods_site o
left anti join (select nvs_core_novartis_unique_id__c,account_odw__c from ph_com_p_usa_veeva.account where ispersonaccount = false and isdeleted = false and recordtypeid = '0121O000001CG7KQAW')
ac on nvl(trim(o.mdm_party_id),'@')=nvl(trim(ac.nvs_core_novartis_unique_id__c),'@') or concat('S',cast(o.site_odw_id as string))=ac.account_odw__c join all_all_e_gbl_customer.idl_mdm_account i on trim(i.customer_id)=trim(o.mdm_party_id) where o.sf_id is null and upper(trim(i.mdm_status_code))='A'
 """)
 
 spark.sql(""" select count(*) from all_all_r_all_bkp.ods_site_t1 """).show

--723
--rec_modify_date as current_timestamp and MDM as rec_modify_by---

spark.sql(""" insert into all_all_r_all_bkp.ods_site_t1  select * from all_all_b_usa_crmods.ods_site c left anti join all_all_r_all_bkp.ods_site_t1 t on trim(t.mdm_party_id)=trim(c.mdm_party_id) """)

spark.sql(""" truncate table all_all_b_usa_crmods.ods_site """)

spark.sql(""" insert into all_all_b_usa_crmods.ods_site select distinct * from all_all_r_all_bkp.ods_site_t1 """)  

-----------------------------------
val :-

spark.sql(""" select rec_modify_date,count(*) from all_all_b_usa_crmods.ods_site group by rec_modify_date order by rec_modify_date desc """)
---will give count of records inserted on sop execution date---


---Jobs :---
EHP_PH_045683_VODACCTV_D

sh /mnt/share/app/proj/all/all/usa/atlas/project_all_all_atlas/bin/atlas_generic_stg_ods.sh acct vodacctv_param qa3

----------------------
EHP_PH_045683_FTVTGACCT_D

sh /mnt/share/app/proj/all/all/usa/atlas/project_all_all_atlas/bin/ftp_infa_out.sh infa VOD_ACCT 1

------------------------------------
--hdfs dfs -ls /sdata/all/all/b/all_all_b_usa_crmods/VantageOutbound/vod_acct_v_/

--hdfs dfs -copyToLocal  /sdata/all/all/b/all_all_b_usa_crmods_qa3/VantageOutbound/vod_acct_v_/vod_acct_v_09302022133839.txt /home/sys_evico_tst_atlas/demo/


