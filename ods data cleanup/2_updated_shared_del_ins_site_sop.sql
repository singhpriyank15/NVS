spark.sql("""create table all_all_r_all_bkp.ods_site_upd_sf_id_bkp like all_all_b_usa_crmods.ods_site""")
spark.sql("""insert into all_all_r_all_bkp.ods_site_upd_sf_id_bkp select * from all_all_b_usa_crmods.ods_site""")


spark.sql("""create table if not exists all_all_r_all_bkp.ods_site_t like all_all_b_usa_crmods.ods_site""")
spark.sql(""" truncate table all_all_r_all_bkp.ods_site_t""")



spark.sql("""with null_upd  as (select o.ff_department,o.cpf_elgbl,o.ms_kam_acct_tier,o.payer_id,o.description,o.cln_rsrch_capable,o.heor_rsrch_capable,o.cln_rsrch_active,o.mangd_market_segmnt,o.heor_rsrch_active,o.org_strata_level,o.center_of_excell,o.mangd_market_phy_grp,o.created_by_id,o.last_modified_id,o.create_date,o.last_modified_date,o.siebel_id,o.midb_site_row_id,o.addr_row_id,o.sonic_create_date,o.sonic_org_type,o.comments,o.site_resident_program_flag,o.site_resident_count,o.site_mpn,o.site_mpn_order,o.site_physician_key,o.site_main_location_flag,o.mdm_party_id,o.edge_id,o.owner_dept,o.usmm_kam,o.tot_rx_lives,o.pbm_crv_out,o.spclty_phrmcy,o.case_mgr,o.num_us_emp,o.ebc,o.chn_fclty_phrmcy_prvdr,o.tot_fclts,o.med_d_beds,o.parta_beds,o.phrmcy_type,o.lvl,o.comm_cntrct_date,o.comm_cntrct_stat,o.medcr_partd_cntrt_date,o.medcr_partd_cntrt_stat,o.medcd_partd_cntrt_date,o.medcd_partd_cntrt_stat,o.tot_covd_lives,o.self_insurd,o.ful_insurd,o.trust,o.top5_geo,o.comm_drug_trnd_prcnt,o.spclty_pmpm,o.spclty_drug_cntr_prcnt,o.tot_sls,o.spclty_tot_prcnt_sls,o.rx_sls,o.tot_nvs_sls,o.nvs_rtns,o.nvs_rtns_prcnt,o.top5_nvs_prod,o.invtry_mnths_on_hnd,o.invtry_as_of_date,o.ex_dspns_rt_prcnt,o.medcr_prcnt,o.medcd_prcnt,o.oth_thrd_prty_prcnt,o.cthmnts,o.departments,o.rsdncy_prgms,o.tot_rx_bdgt,o.add_degs,o.job_ttl,o.plntrk_id,o.hsg_id,o.hlth_ldrs_id,o.dea_no,o.ftf_parent_mdm_id,o.onc_cntrt_date,o.onc_cntrt_stat,o.pbm_tot_lives,o.emplr,o.hmo_ppo,o.bcbs,o.medcr_mm_pbm,o.oth_fed,o.other,o.tot_of_phrm,o.cntr_date,o.cntrt_stat,o.spclty_drug_trnd,o.onc_kam,o.tot_rev,o.net_inc_loss,o.medc_loss_ratio,o.comm_prem_pmpm,o.ftf_prvdr_mdm_id,o.mdm_src,o.cust_excptn_flag,o.cust_key,o.mdm_src_id,o.mdm_cust_subtype,o.mdm_cust_type,o.mm_kam_acct_tier,o.exec_sum,o.fully_insured_rx_lives,o.comm_insured_rx_lives,o.comm_self_insured_rx_lives,o.med_caid,o.med_care,o.unos_id,o.alias_name,o.future_coe__c,o.attst_prod,o.msl_site_name,o.group_s,o.portfolio_msl_contact,o.lcz_attstn,o.lcz_attst_prod,o.xolair_attst_prod,o.prod_attstn,o.compliance_attstn,o.stt_and_fed_law_attstn,o.derm_rheum_clinic__c,o.site_odw_id,o.site_vendor_org_id,o.cam_key,ac.id sf_id,o.sf_is_deleted,o.sf_owner_id,o.acct_odw_id,o.cam_acct_key,o.site_name,o.site_dba_name,o.formatted_nm,o.hin,o.ddd_no,o.ddd_sub_cat_code,o.cust_type,o.aha,o.birth_date,o.bus,o.bus_desc,o.contracts_prcs,o.credentials,o.do_not_sync_sales_data,o.employees,o.excld_from_terr_asgnmt_rules,o.excld_from_zip_to_terr_prcsg,o.fax,o.fax_2,o.fellowship_location,o.include_in_terr_asgnmt,o.may_edit,o.me,o.modl,o.net_income_loss,o.npi,o.oth_rel_info,o.ownership,o.pager,o.phm_24_hr_flag,o.phm_disp_cls_code,o.phm_disp_type_code,o.phm_store_no,o.site_phn_1,o.site_phn_2,o.programs,o.regional_strategy,o.site_census_beds,o.site_cot_class,o.site_cot_facility_type,o.site_fips_county,o.site_fips_state,o.site_license_beds,o.site_msa,o.site_or_surgeries,o.site_procedures,o.site_profit_status,o.site_segment,o.site_staffed_beds,o.site_status_indicator,o.site_surgeries,o.site_web_site_url,o.spend_cap,o.src,o.staff_notes,o.tax_stat,o.teach_hospital_flag,o.territories,o.totl_bed_cnt,o.totl_lives,o.totl_mds_dos,o.totl_orgc_surg,o.totl_phrmts,o.totl_phys_enrolled,o.totl_revenue,o.tumor_types,o.veterans_affairs_hospital,o.visn,o.ytd_calls,o.ytd_stt_spend,o.inactive,o.rec_insert_by,o.rec_insert_date,o.rec_modify_date,o.batch_id_insert,o.centre_no,o.sonic_id,o.dea,o.is_deleted,o.sonic_org_ext_row_id,o.phone,o.email,o.dept_no,o.totl_site,o.site_cmi,o.site_cot_specialty,o.hem_msl_contact,o.onc_msl_contact,o.rd_msl_contact,o.academic_community,o.pc_kam_key_account,o.pam_key_account,o.request_code,o.patnt_intr,o.site_agmt_revw_date,o.scz_vldt_stat,o.sec_scz_vldt_stat,o.brd_cert_in,o.key_oso_mm_contact_nov,o.contacts_identifier,o.tobi_validation_status,o.site_agmt_revw_with,o.rec_modify_by,o.batch_id_update,o.nvs_core_profit_status__c,o.beds__c,o.country_vod__c,o.external_id_vod__c,o.nvs_core_account_category__c,o.nvs_core_account_subtype__c,o.nvs_core_credentialing_required__c,o.nvs_core_credentialing_service_company__c,o.nvs_core_digital_id__c,o.nvs_core_emr_system__c,o.nvs_core_fiscal_id__c,o.nvs_core_key_account__c,o.nvs_core_legal_name__c,o.nvs_core_national_code__c,o.nvs_core_pricing_program__c,o.nvs_core_salesforce_id__c,o.nvs_core_sap_id__c,o.nvs_core_status__c,o.nvs_core_top_account_name__c,o.nvs_core_vendor_id__c,o.primary_parent_vod__c,o.recordtype,o.specialty_1_vod__c,o.specialty_2_vod__c,o.hco_phone from all_all_b_usa_crmods.ods_site o
 join (select id,nvs_core_novartis_unique_id__c,account_odw__c from ph_com_p_usa_veeva.account where ispersonaccount = false and isdeleted = false and recordtypeid = '0121O000001CG7KQAW')
ac on nvl(trim(o.mdm_party_id),'@')=nvl(trim(ac.nvs_core_novartis_unique_id__c),'@') or concat('S',cast(o.site_odw_id as string))=ac.account_odw__c join all_all_e_gbl_customer.idl_mdm_account i on trim(i.customer_id)=trim(o.mdm_party_id) where o.sf_id is null and upper(trim(i.mdm_status_code))='A')

insert into all_all_r_all_bkp.ods_site_t select * from null_upd""")


spark.sql("""insert into all_all_r_all_bkp.ods_site_t select ods.* from all_all_b_usa_crmods.ods_site ods left anti join all_all_r_all_bkp.ods_site_t t on trim(t.mdm_party_id)=trim(ods.mdm_party_id)""")

spark.sql(""" truncate table all_all_b_usa_crmods.ods_site """)

spark.sql(""" insert into all_all_b_usa_crmods.ods_site select distinct * from all_all_r_all_bkp.ods_site_t """)  





-- validation

spark.sql("select o.* from all_all_b_usa_crmods.ods_site o join (select id,nvs_core_novartis_unique_id__c,account_odw__c from ph_com_p_usa_veeva.account where ispersonaccount = false and isdeleted = false and recordtypeid = '0121O000001CG7KQAW') ac on nvl(trim(o.mdm_party_id),'@')=nvl(trim(ac.nvs_core_novartis_unique_id__c),'@') or concat('S',cast(o.site_odw_id as string))=ac.account_odw__c join all_all_e_gbl_customer.idl_mdm_account i on trim(i.customer_id)=trim(o.mdm_party_id) where o.sf_id is null and upper(trim(i.mdm_status_code))='A' ").show

spark.sql("select count(1) from all_all_r_all_bkp.ods_site_t").show
spark.sql("select sf_id from all_all_r_all_bkp.ods_site_t where sf_id is null").show

