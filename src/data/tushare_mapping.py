#!/usr/bin/env python3
"""Tushare接口字段映射配置

此文件定义了Tushare API字段名到标准财务字段名的映射关系
用于在LineItem模型中使用统一的字段命名
"""

# 现金流量表字段映射 (pro.cashflow)
CASHFLOW_FIELD_MAPPING = {
    # 基础信息字段
    'ts_code': 'ts_code',
    'ann_date': 'ann_date', 
    'f_ann_date': 'f_ann_date',
    'end_date': 'end_date',
    'comp_type': 'comp_type',
    'report_type': 'report_type',
    
    # 现金流量表主要项目
    'net_profit': 'net_profit',                          # 净利润
    'finan_exp': 'finance_expense',                      # 财务费用
    'c_fr_sale_sg': 'cash_from_sales',                   # 销售商品、提供劳务收到的现金
    'recp_tax_retu': 'cash_from_tax_refunds',            # 收到的税费返还
    'n_depos_incr_fi': 'net_deposit_increase',           # 客户存款和同业存放款项净增加额
    'n_incr_loans_cb': 'net_loans_from_cb',              # 向中央银行借款净增加额
    'n_inc_borr_oth_fi': 'net_borrowing_from_other_fi',  # 向其他金融机构拆入资金净增加额
    'prem_fr_orig_contr': 'premiums_from_original_contracts', # 收到原保险合同保费取得的现金
    'n_incr_insured_dep': 'net_insured_deposit_increase', # 保户储金净增加额
    'n_reinsur_prem': 'net_reinsurance_premiums',        # 收到再保业务现金净额
    'n_incr_disp_tfa': 'net_increase_in_tfa',            # 处置交易性金融资产净增加额
    'ifc_cash_incr': 'interfund_cash_increase',          # 代理买卖证券收到的现金净额
    'n_incr_disp_faas': 'net_increase_in_faas',          # 处置可供出售金融资产净增加额
    'n_incr_disc_notes': 'net_increase_in_discount_notes', # 拆出资金净增加额
    'c_disp_withdrwl_invest': 'cash_from_investments',    # 收回投资收到的现金
    'c_recp_return_invest': 'cash_from_investment_returns', # 取得投资收益收到的现金
    'n_recp_disp_fiolta': 'net_disposal_of_ppe',         # 处置固定资产、无形资产和其他长期资产收回的现金净额
    'n_recp_disp_sobu': 'net_disposal_of_subsidiaries',  # 处置子公司及其他营业单位收到的现金净额
    'stot_inflows_inv_act': 'total_cash_inflows_investing', # 投资活动现金流入小计
    'c_pay_acq_const_fiolta': 'capital_expenditure',     # 购建固定资产、无形资产和其他长期资产支付的现金
    'c_paid_invest': 'cash_paid_for_investments',        # 投资支付的现金
    'n_incr_pledge_loan': 'net_increase_in_pledge_loans', # 质押贷款净增加额
    'n_incr_disp_tfa_invest': 'net_increase_in_tfa_invest', # 处置交易性金融资产净增加额
    'c_pay_acq_sobu': 'cash_paid_for_acquisitions',      # 取得子公司及其他营业单位支付的现金净额
    'stot_out_inv_act': 'total_cash_outflows_investing',  # 投资活动现金流出小计
    'n_cashflow_inv_act': 'investing_cash_flow',          # 投资活动产生的现金流量净额
    'c_recp_borrow': 'cash_from_borrowing',               # 取得借款收到的现金
    'proc_issue_bonds': 'proceeds_from_bonds',            # 发行债券收到的现金
    'oth_cash_recp_ral_fnc_act': 'other_financing_inflows', # 筹资活动收到的其他现金
    'stot_cash_in_fnc_act': 'total_cash_inflows_financing', # 筹资活动现金流入小计
    'free_cashflow': 'free_cash_flow',                    # 企业自由现金流量
    'c_prepay_amt_borr': 'cash_prepaid_borrowing',        # 偿还债务支付的现金
    'c_pay_dist_dpcp_int_exp': 'dividends_and_other_cash_distributions', # 分配股利、利润或偿付利息支付的现金
    'incl_dvd_profit_paid_sc_ms': 'dividends_paid_to_minorities', # 其中:子公司支付给少数股东的股利、利润
    'oth_cashpay_ral_fnc_act': 'other_financing_outflows', # 筹资活动支付的其他现金
    'stot_cashout_fnc_act': 'total_cash_outflows_financing', # 筹资活动现金流出小计
    'n_cash_flows_fnc_act': 'financing_cash_flow',        # 筹资活动产生的现金流量净额
    'eff_fx_flu_cash': 'fx_effects_on_cash',              # 汇率变动对现金的影响
    'n_incr_cash_cash_equ': 'net_cash_increase',          # 现金及现金等价物净增加额
    'c_cash_equ_beg_period': 'cash_beginning_period',     # 期初现金及现金等价物余额
    'c_cash_equ_end_period': 'cash_and_equivalents',      # 期末现金及现金等价物余额
    'c_recp_cap_contrib': 'cash_from_capital_contributions', # 吸收投资收到的现金
    'incl_cash_rec_saims': 'cash_from_subsidiaries',      # 其中:子公司吸收少数股东投资收到的现金
    'uncon_invest_loss': 'unconsolidated_investment_loss', # 对联营企业和合营企业的投资损失
    'prov_depr_assets': 'provision_for_asset_depreciation', # 资产减值准备
    'depr_fa_coga_dpba': 'depreciation_of_ppe',           # 固定资产折旧、油气资产折耗、生产性生物资产折旧
    'amort_intang_assets': 'amortization_of_intangibles', # 无形资产摊销
    'lt_amort_deferred_exp': 'amortization_of_deferred_exp', # 长期待摊费用摊销
    'decr_deferred_exp': 'decrease_in_deferred_exp',      # 待摊费用减少
    'incr_acc_exp': 'increase_in_accrued_exp',            # 预提费用增加
    'loss_disp_fiolta': 'loss_on_disposal_of_ppe',       # 处置固定、无形资产和其他长期资产的损失
    'loss_scr_fa': 'loss_on_scrapping_of_fa',            # 固定资产报废损失
    'loss_fv_chg': 'loss_on_fair_value_changes',         # 公允价值变动损失
    'invest_loss': 'investment_loss',                     # 投资损失
    'decr_def_inc_tax_assets': 'decrease_in_deferred_tax_assets', # 递延所得税资产减少
    'incr_def_inc_tax_liab': 'increase_in_deferred_tax_liab', # 递延所得税负债增加
    'decr_inventories': 'decrease_in_inventories',        # 存货的减少
    'decr_oper_payable': 'decrease_in_operating_receivables', # 经营性应收项目的减少
    'incr_oper_payable': 'increase_in_operating_payables', # 经营性应付项目的增加
    'others': 'other_operating_adjustments',              # 其他
    'im_net_cashflow_oper_act': 'operating_cash_flow_indirect', # 经营活动产生的现金流量净额(间接法)
    'conv_debt_into_cap': 'debt_to_equity_conversion',    # 债务转为资本
    'conv_copbonds_due_within_1y': 'convertible_bonds_due_1y', # 一年内到期的可转换公司债券
    'fa_fnc_leases': 'finance_lease_fixed_assets',        # 融资租入固定资产
    'im_n_incr_cash_equ': 'net_cash_increase_indirect',   # 现金及现金等价物净增加额(间接法)
    'net_dism_capital_add': 'net_capital_reduction',      # 拆出资金净增加额
    'net_cash_rece_sec': 'net_cash_from_securities',      # 代理买卖证券收到的现金净额
    'credit_impa_loss': 'credit_impairment_loss',         # 信用减值损失
    'use_right_asset_dep': 'right_of_use_asset_depreciation', # 使用权资产折旧
    'oth_loss_asset': 'other_asset_impairment_loss',      # 其他资产减值损失
    'end_bal_cash': 'ending_cash_balance',                # 现金的期末余额
    'beg_bal_cash': 'beginning_cash_balance',             # 减:现金的期初余额
    'end_bal_cash_equ': 'ending_cash_equivalent_balance', # 加:现金等价物的期末余额
    'beg_bal_cash_equ': 'beginning_cash_equivalent_balance', # 减:现金等价物的期初余额
    'update_flag': 'update_flag',                         # 更新标志
    
    # 经营活动现金流
    'n_cashflow_act': 'operating_cash_flow',              # 经营活动产生的现金流量净额
    'c_paid_goods_s': 'cash_paid_for_goods',             # 购买商品、接受劳务支付的现金
    'c_paid_to_for_empl': 'cash_paid_to_employees',      # 支付给职工以及为职工支付的现金
    'c_paid_for_taxes': 'cash_paid_for_taxes',           # 支付的各项税费
    'n_incr_cash_cash_equ': 'net_cash_increase',         # 现金及现金等价物净增加额
}

# 利润表字段映射 (pro.income)
INCOME_FIELD_MAPPING = {
    'ts_code': 'ts_code',
    'ann_date': 'ann_date',
    'f_ann_date': 'f_ann_date', 
    'end_date': 'end_date',
    'report_type': 'report_type',
    'comp_type': 'comp_type',
    'basic_eps': 'basic_eps',                            # 基本每股收益
    'diluted_eps': 'diluted_eps',                        # 稀释每股收益
    'total_revenue': 'total_revenue',                    # 营业总收入
    'revenue': 'revenue',                                # 营业收入
    'int_income': 'interest_income',                     # 利息收入
    'prem_earned': 'premiums_earned',                    # 已赚保费
    'comm_income': 'commission_income',                  # 手续费及佣金收入
    'n_commis_income': 'net_commission_income',          # 手续费及佣金收入净额
    'n_oth_income': 'other_operating_income',            # 其他经营收入
    'n_oth_b_income': 'other_business_income',           # 加:其他业务收入
    'prem_income': 'premium_income',                     # 保险业务收入
    'out_prem': 'outward_premiums',                      # 减:分出保费
    'une_prem_reser': 'unearned_premium_reserves',       # 减:提取未到期责任准备金
    'reins_income': 'reinsurance_income',                # 再保险业务收入
    'n_sec_tb_income': 'net_securities_trading_income',  # 证券交易收入
    'n_sec_uw_income': 'net_securities_underwriting_income', # 证券承销收入
    'n_asset_mg_income': 'net_asset_management_income',  # 受托客户资产管理业务收入
    'oth_b_income': 'other_business_income_total',       # 其他业务收入
    'fv_value_chg_gain': 'fair_value_change_gains',      # 公允价值变动收益
    'invest_income': 'investment_income',                # 投资收益
    'ass_invest_income': 'associate_investment_income',  # 对联营企业和合营企业的投资收益
    'forex_gain': 'foreign_exchange_gains',              # 汇兑收益
    'total_cogs': 'total_cost_of_goods_sold',            # 营业总成本
    'oper_cost': 'operating_costs',                      # 营业成本
    'int_exp': 'interest_expense',                       # 利息支出
    'comm_exp': 'commission_expense',                    # 手续费及佣金支出
    'biz_tax_surchg': 'business_tax_and_surcharges',     # 营业税金及附加
    'sell_exp': 'selling_expenses',                      # 销售费用
    'admin_exp': 'administrative_expenses',              # 管理费用
    'fin_exp': 'finance_expenses',                       # 财务费用
    'assets_impair_loss': 'asset_impairment_losses',     # 资产减值损失
    'prem_refund': 'premium_refunds',                    # 退保金
    'compens_payout': 'compensation_payouts',            # 赔付支出净额
    'reser_insur_liab': 'insurance_liability_reserves',  # 提取保险责任准备金
    'div_payt': 'dividend_payments',                     # 保户红利支出
    'reins_exp': 'reinsurance_expenses',                 # 分保费用
    'oper_exp': 'operating_expense',                     # 营业支出
    'compens_payout_refu': 'compensation_payout_refunds', # 摊回赔付支出
    'insur_reser_refu': 'insurance_reserve_refunds',     # 摊回保险责任准备金
    'reins_cost_refund': 'reinsurance_cost_refunds',     # 摊回分保费用
    'other_bus_cost': 'other_business_costs',            # 其他业务成本
    'operate_profit': 'operating_profit',                # 营业利润
    'non_oper_income': 'non_operating_income',           # 营业外收入
    'non_oper_exp': 'non_operating_expenses',            # 营业外支出
    'nca_disploss': 'non_current_asset_disposal_loss',   # 非流动资产处置损失
    'total_profit': 'total_profit',                      # 利润总额
    'income_tax': 'income_tax_expense',                  # 所得税费用
    'n_income': 'net_income',                            # 净利润
    'n_income_attr_p': 'net_income_attributable_to_parent', # 归属于母公司所有者的净利润
    'minority_gain': 'minority_interests',               # 少数股东损益
    'oth_compr_income': 'other_comprehensive_income',    # 其他综合收益
    'ebit': 'ebit',                                      # 息税前利润
    'ebitda': 'ebitda',                                  # 息税折旧摊销前利润
    'update_flag': 'update_flag',                        # 更新标志
}

# 资产负债表字段映射 (pro.balancesheet)
BALANCE_FIELD_MAPPING = {
    'ts_code': 'ts_code',
    'ann_date': 'ann_date',
    'f_ann_date': 'f_ann_date',
    'end_date': 'end_date',
    'report_type': 'report_type',
    'comp_type': 'comp_type',
    'total_share': 'total_shares',                       # 期末总股本
    'cap_rese': 'capital_reserves',                      # 资本公积金
    'undistr_porfit': 'undistributed_profits',           # 未分配利润
    'surplus_rese': 'surplus_reserves',                  # 盈余公积金
    'special_rese': 'special_reserves',                  # 专项储备
    'money_cap': 'monetary_capital',                     # 货币资金
    'trad_asset': 'trading_assets',                      # 交易性金融资产
    'notes_receiv': 'notes_receivable',                  # 应收票据
    'accounts_receiv': 'accounts_receivable',            # 应收账款
    'oth_receiv': 'other_receivables',                   # 其他应收款
    'prepayment': 'prepaid_expenses',                    # 预付款项
    'div_receiv': 'dividends_receivable',                # 应收股利
    'int_receiv': 'interest_receivable',                 # 应收利息
    'inventories': 'inventories',                        # 存货
    'amor_exp': 'deferred_expenses',                     # 待摊费用
    'nca_within_1y': 'non_current_assets_due_within_1y', # 一年内到期的非流动资产
    'sett_rsrv': 'settlement_reserves',                  # 结算备付金
    'loanto_oth_bank_fi': 'loans_to_other_banks',        # 拆出资金
    'premium_receiv': 'premiums_receivable',             # 应收保费
    'reinsur_receiv': 'reinsurance_receivables',         # 应收分保账款
    'reinsur_res_receiv': 'reinsurance_reserve_receivables', # 应收分保未到期责任准备金
    'pur_resale_fa': 'financial_assets_purchased_for_resale', # 买入返售金融资产
    'oth_cur_assets': 'other_current_assets',            # 其他流动资产
    'total_cur_assets': 'current_assets',                # 流动资产合计
    'fa_avail_for_sale': 'available_for_sale_financial_assets', # 可供出售金融资产
    'htm_invest': 'held_to_maturity_investments',        # 持有至到期投资
    'lt_eqt_invest': 'long_term_equity_investments',     # 长期股权投资
    'invest_real_estate': 'investment_real_estate',      # 投资性房地产
    'time_deposits': 'time_deposits',                    # 定期存款
    'oth_assets': 'other_assets',                        # 其他资产
    'lt_rec': 'long_term_receivables',                   # 长期应收款
    'fix_assets': 'fixed_assets',                        # 固定资产
    'cip': 'construction_in_progress',                   # 在建工程
    'const_materials': 'construction_materials',         # 工程物资
    'fixed_assets_disp': 'fixed_assets_for_disposal',    # 固定资产清理
    'produc_bio_assets': 'productive_biological_assets', # 生产性生物资产
    'oil_and_gas_assets': 'oil_and_gas_assets',         # 油气资产
    'intang_assets': 'intangible_assets',                # 无形资产
    'r_and_d': 'research_and_development',               # 开发支出
    'goodwill': 'goodwill',                              # 商誉
    'lt_amor_exp': 'long_term_deferred_expenses',        # 长期待摊费用
    'defer_tax_assets': 'deferred_tax_assets',           # 递延所得税资产
    'decr_in_disbur': 'decrease_in_disbursements',       # 发放贷款及垫款
    'oth_nca': 'other_non_current_assets',               # 其他非流动资产
    'total_nca': 'total_non_current_assets',             # 非流动资产合计
    'cash_reser_cb': 'cash_reserves_with_central_bank',  # 向中央银行上缴法定准备金
    'depos_in_oth_bfi': 'deposits_in_other_banks',       # 存放同业款项
    'prec_metals': 'precious_metals',                    # 贵金属
    'deriv_assets': 'derivative_assets',                 # 衍生金融资产
    'rr_reins_une_prem': 'reinsurance_unearned_premiums', # 应收分保未到期责任准备金
    'rr_reins_outstd_cla': 'reinsurance_outstanding_claims', # 应收分保未决赔款准备金
    'rr_reins_lins_liab': 'reinsurance_life_insurance_liability', # 应收分保寿险责任准备金
    'rr_reins_lthins_liab': 'reinsurance_long_term_health_insurance_liability', # 应收分保长期健康险责任准备金
    'refund_depos': 'refundable_deposits',               # 存出保证金
    'ph_pledge_loans': 'pledged_loans',                  # 保户质押贷款
    'receiv_invest': 'receivable_investments',           # 应收款项投资
    'use_right_assets': 'right_of_use_assets',           # 使用权资产
    'oth_cur_assets_spec': 'other_current_assets_special', # 其他流动资产(特殊项目)
    'oth_nca_spec': 'other_non_current_assets_special',  # 其他非流动资产(特殊项目)
    'total_assets': 'total_assets',                      # 资产总计
    'lt_borr': 'long_term_borrowings',                   # 长期借款
    'st_borr': 'short_term_borrowings',                  # 短期借款
    'cb_borr': 'central_bank_borrowings',                # 向中央银行借款
    'depos_ib_deposits': 'interbank_deposits',           # 吸收存款及同业存放
    'loan_oth_bank': 'loans_from_other_banks',           # 拆入资金
    'trading_fl': 'trading_financial_liabilities',       # 交易性金融负债
    'notes_payable': 'notes_payable',                    # 应付票据
    'acct_payable': 'accounts_payable',                  # 应付账款
    'adv_receipts': 'advance_receipts',                  # 预收款项
    'sold_for_repur_fa': 'financial_assets_sold_for_repurchase', # 卖出回购金融资产款
    'comm_payable': 'commission_payable',                # 应付手续费及佣金
    'payroll_payable': 'payroll_payable',                # 应付职工薪酬
    'taxes_payable': 'taxes_payable',                    # 应交税费
    'int_payable': 'interest_payable',                   # 应付利息
    'div_payable': 'dividends_payable',                  # 应付股利
    'oth_payable': 'other_payables',                     # 其他应付款
    'acc_exp': 'accrued_expenses',                       # 预提费用
    'deferred_inc': 'deferred_income',                   # 递延收益
    'st_bonds_payable': 'short_term_bonds_payable',      # 应付短期债券
    'payable_to_reinsurer': 'payable_to_reinsurer',      # 应付分保账款
    'rsrv_insur_cont': 'insurance_contract_reserves',    # 保险合同准备金
    'acting_trading_sec': 'acting_trading_securities',   # 代理买卖证券款
    'acting_uw_sec': 'acting_underwriting_securities',   # 代理承销证券款
    'non_cur_liab_due_1y': 'non_current_liabilities_due_within_1y', # 一年内到期的非流动负债
    'oth_cur_liab': 'other_current_liabilities',         # 其他流动负债
    'total_cur_liab': 'current_liabilities',             # 流动负债合计
    'bond_payable': 'bonds_payable',                     # 应付债券
    'lt_payable': 'long_term_payables',                  # 长期应付款
    'specific_payables': 'specific_payables',            # 专项应付款
    'estimated_liab': 'estimated_liabilities',           # 预计负债
    'defer_tax_liab': 'deferred_tax_liabilities',        # 递延所得税负债
    'defer_inc_non_cur_liab': 'deferred_income_non_current', # 递延收益-非流动负债
    'oth_ncl': 'other_non_current_liabilities',          # 其他非流动负债
    'total_ncl': 'total_non_current_liabilities',        # 非流动负债合计
    'depos_oth_bfi': 'deposits_from_other_banks',        # 同业和其他金融机构存放款项
    'deriv_liab': 'derivative_liabilities',              # 衍生金融负债
    'depos': 'customer_deposits',                        # 吸收存款
    'agency_bus_liab': 'agency_business_liabilities',    # 代理业务负债
    'oth_liab': 'other_liabilities',                     # 其他负债
    'prem_receiv_adva': 'premiums_received_in_advance',  # 预收保费
    'depos_received': 'deposits_received',               # 存入保证金
    'ph_invest': 'policyholder_investments',             # 保户储金及投资款
    'reser_une_prem': 'unearned_premium_reserves_liab',  # 未到期责任准备金
    'reser_outstd_claims': 'outstanding_claims_reserves', # 未决赔款准备金
    'reser_lins_liab': 'life_insurance_liability_reserves', # 寿险责任准备金
    'reser_lthins_liab': 'long_term_health_insurance_reserves', # 长期健康险责任准备金
    'indept_acc_liab': 'independent_account_liabilities', # 独立账户负债
    'pledge_borr': 'pledged_borrowings',                 # 其中:质押借款
    'indem_payable': 'indemnity_payable',                # 应付赔付款
    'policy_div_payable': 'policy_dividend_payable',     # 应付保单红利
    'total_liab': 'total_liabilities',                   # 负债合计
    'treasury_share': 'treasury_shares',                 # 减:库存股
    'ordin_risk_reser': 'ordinary_risk_reserves',        # 一般风险准备
    'forex_differ': 'foreign_exchange_differences',      # 外币报表折算差额
    'invest_loss_unconf': 'unconfirmed_investment_losses', # 未确认的投资损失
    'minority_int': 'minority_interests_equity',         # 少数股东权益
    'total_hldr_eqy_exc_min_int': 'shareholders_equity', # 股东权益合计(不含少数股东权益)
    'total_hldr_eqy_inc_min_int': 'total_equity_including_minority', # 股东权益合计(含少数股东权益)
    'total_liab_hldr_eqy': 'total_liabilities_and_equity', # 负债及股东权益总计
    'lt_payroll_payable': 'long_term_payroll_payable',   # 长期应付职工薪酬
    'oth_comp_income': 'other_comprehensive_income_equity', # 其他综合收益
    'oth_eqt_tools': 'other_equity_instruments',         # 其他权益工具
    'oth_eqt_tools_p_shr': 'other_equity_instruments_preferred', # 其中:优先股
    'lending_funds': 'lending_funds',                    # 放出贷款
    'acc_receivable': 'accounts_receivable_alt',         # 应收账款
    'st_fin_payable': 'short_term_financial_payables',   # 短期应付款项
    'payables': 'total_payables',                        # 应付款项
    'hfs_assets': 'held_for_sale_assets',                # 持有待售资产
    'hfs_sales': 'held_for_sale_liabilities',            # 持有待售负债
    'cost_fin_assets': 'cost_method_financial_assets',   # 以成本计量的可供出售金融资产
    'fair_value_fin_assets': 'fair_value_financial_assets', # 以公允价值计量且其变动计入其他综合收益的金融资产
    'cip_total': 'construction_in_progress_total',       # 在建工程(合计)
    'oth_pay_total': 'other_payables_total',             # 其他应付款(合计)
    'long_pay_total': 'long_term_payables_total',        # 长期应付款(合计)
    'debt_invest': 'debt_investments',                   # 债权投资
    'oth_debt_invest': 'other_debt_investments',         # 其他债权投资
    'oth_eq_invest': 'other_equity_investments',         # 其他权益工具投资
    'oth_illiq_fin_assets': 'other_non_liquid_financial_assets', # 其他非流动金融资产
    'oth_eq_ppbond': 'other_equity_perpetual_bonds',     # 其中:永续债
    'receiv_financing': 'receivables_financing',         # 应收款项融资
    'use_right_assets_spec': 'right_of_use_assets_special', # 使用权资产(特殊项目)
    'lease_liab': 'lease_liabilities',                   # 租赁负债
    'contract_assets': 'contract_assets',                # 合同资产
    'contract_liab': 'contract_liabilities',             # 合同负债
    'accounts_receiv_bill': 'accounts_receivable_and_bills', # 应收账款及应收票据
    'accounts_pay': 'accounts_payable_alt',              # 应付账款
    'oth_rcv_total': 'other_receivables_total',          # 其他应收款(合计)
    'fix_assets_total': 'fixed_assets_total',            # 固定资产(合计)
    'update_flag': 'update_flag',                        # 更新标志
}

# 财务指标字段映射 (pro.fina_indicator)
FINANCIAL_METRICS_FIELD_MAPPING = {
    'ts_code': 'ts_code',
    'ann_date': 'ann_date',
    'end_date': 'end_date',
    'eps': 'earnings_per_share',                         # 基本每股收益
    'dt_eps': 'diluted_earnings_per_share',              # 稀释每股收益
    'total_revenue_ps': 'revenue_per_share',             # 每股营业总收入
    'revenue_ps': 'revenue_per_share_alt',               # 每股营业收入
    'capital_rese_ps': 'capital_reserve_per_share',      # 每股资本公积
    'surplus_rese_ps': 'surplus_reserve_per_share',      # 每股盈余公积
    'undist_profit_ps': 'undistributed_profit_per_share', # 每股未分配利润
    'extra_item': 'extraordinary_items',                 # 非经常性损益
    'profit_dedt': 'profit_after_extraordinary_items',   # 扣除非经常性损益后的净利润
    'gross_margin': 'gross_margin',                      # 毛利
    'current_ratio': 'current_ratio',                    # 流动比率
    'quick_ratio': 'quick_ratio',                        # 速动比率
    'cash_ratio': 'cash_ratio',                          # 保守速动比率
    'invturn_days': 'inventory_turnover_days',           # 存货周转天数
    'arturn_days': 'accounts_receivable_turnover_days',  # 应收账款周转天数
    'inv_turn': 'inventory_turnover',                    # 存货周转率
    'ar_turn': 'accounts_receivable_turnover',           # 应收账款周转率
    'ca_turn': 'current_assets_turnover',                # 流动资产周转率
    'fa_turn': 'fixed_assets_turnover',                  # 固定资产周转率
    'assets_turn': 'total_assets_turnover',              # 总资产周转率
    'roe': 'return_on_equity',                           # 净资产收益率
    'npta': 'net_profit_to_total_assets',                # 总资产报酬率
    'op_income': 'operating_income',                     # 经营活动净收益
    'daa': 'depreciation_and_amortization',              # 折旧与摊销
    'roic': 'roic',                                      # 投入资本回报率
    'roe_waa': 'roe',                                    # 加权平均净资产收益率
    'roe_dt': 'return_on_equity_diluted',                # 稀释净资产收益率
    'roe_yearly': 'return_on_equity_yearly',             # 年化净资产收益率
    'roa': 'return_on_assets',                           # 总资产净利率
    'npta_yearly': 'net_profit_to_assets_yearly',        # 年化总资产净利率
    'roa_yearly': 'roa',                                 # 年化总资产收益率
    'roa_dp': 'return_on_assets_dp',                     # 总资产收益率(扣除非经常损益)
    'cf_sales': 'cash_flow_to_sales',                    # 经营现金净流量对销售收入比率
    'roa_yearly_2': 'return_on_assets_yearly_2',         # 年化总资产收益率(二)
    'roa_dp_2': 'return_on_assets_dp_2',                 # 总资产收益率(扣除非经常损益)(二)
    'cf_nm': 'cash_flow_to_net_income',                  # 经营现金净流量与净利润的比率
    'cf_liabs': 'cash_flow_to_liabilities',              # 经营现金净流量对负债比率
    'cashflow_m': 'cash_flow_margin',                    # 现金流量比率
    'op_of_gr': 'operating_margin',                      # 营业利润/营业总收入
    'debt_to_assets': 'debt_to_assets',                  # 资产负债率
    'assets_to_eqt': 'assets_to_equity',                 # 权益乘数
    'dp_assets_to_eqt': 'dp_assets_to_equity',           # 权益乘数(杜邦分析)
    'ca_to_assets': 'current_assets_to_total_assets',    # 流动资产/总资产
    'nca_to_assets': 'non_current_assets_to_total_assets', # 非流动资产/总资产
    'tbassets_to_totalassets': 'tangible_assets_to_total_assets', # 有形资产/总资产
    'int_to_talcap': 'interest_bearing_debt_to_total_capital', # 带息债务/全部投入资本
    'eqt_to_talcapital': 'equity_to_total_capital',      # 归属于母公司的股东权益/全部投入资本
    'currentdebt_to_debt': 'current_debt_to_total_debt', # 流动负债/负债合计
    'longdeb_to_debt': 'long_term_debt_to_total_debt',   # 非流动负债/负债合计
    'ocf_to_shortdebt': 'operating_cash_flow_to_short_debt', # 经营活动产生的现金流量净额/流动负债
    'debt_to_eqt': 'debt_to_equity',                     # 产权比率
    'eqt_to_debt': 'equity_to_debt',                     # 归属于母公司的股东权益/负债合计
    'eqt_to_interestdebt': 'equity_to_interest_bearing_debt', # 归属于母公司的股东权益/带息债务
    'tangibleasset_to_debt': 'tangible_assets_to_debt',  # 有形资产/负债合计
    'tangasset_to_intdebt': 'tangible_assets_to_interest_debt', # 有形资产/带息债务
    'tangibleasset_to_netdebt': 'tangible_assets_to_net_debt', # 有形资产/净债务
    'ocf_to_debt': 'operating_cash_flow_to_debt',        # 经营活动产生的现金流量净额/负债合计
    'ocf_to_interestdebt': 'operating_cash_flow_to_interest_debt', # 经营活动产生的现金流量净额/带息债务
    'ocf_to_netdebt': 'operating_cash_flow_to_net_debt', # 经营活动产生的现金流量净额/净债务
    'ebit_to_interest': 'ebit_to_interest_expense',      # EBIT/利息费用
    'longdebt_to_workingcapital': 'long_debt_to_working_capital', # 长期债务与营运资金比率
    'ebitda_to_debt': 'ebitda_to_debt',                  # EBITDA/负债合计
    'turn_days': 'turnover_days',                        # 营业周期
    'roa_yearly_3': 'return_on_assets_yearly_3',         # 年化总资产收益率(三)
    'roa_dp_3': 'return_on_assets_dp_3',                 # 总资产收益率(扣除非经常损益)(三)
    'fixed_assets': 'fixed_assets_ratio',                # 固定资产合计
    'profit_prefin_exp': 'profit_before_finance_expense', # 息税前利润/营业总收入
    'non_op_profit': 'non_operating_profit_ratio',       # 非营业利润比重
    'op_to_ebt': 'operating_profit_to_ebt',              # 营业利润/利润总额
    'tax_to_ebt': 'tax_to_ebt',                          # 所得税/利润总额
    'dtprofit_to_profit': 'deferred_tax_to_profit',      # 扣除非经常损益后的净利润/净利润
    'salescash_to_or': 'sales_cash_to_operating_revenue', # 销售商品提供劳务收到的现金/营业收入
    'ocf_to_or': 'operating_cash_flow_to_revenue',       # 经营活动产生的现金流量净额/营业收入
    'ocf_to_opincome': 'operating_cash_flow_to_operating_income', # 经营活动产生的现金流量净额/经营活动净收益
    'capitalized_to_da': 'capitalized_to_depreciation',  # 资本化支出/折旧和摊销
    'debt_to_assets_2': 'debt_to_assets_2',              # 资产负债率(二)
    'assets_to_eqt_2': 'assets_to_equity_2',             # 权益乘数(二)
    'dp_assets_to_eqt_2': 'dp_assets_to_equity_2',       # 权益乘数(杜邦分析)(二)
    'profit_to_op': 'profit_to_operating_revenue',       # 利润总额/营业收入
    'q_opincome': 'quarterly_operating_income',          # 经营活动净收益
    'q_investincome': 'quarterly_investment_income',     # 价值变动净收益
    'q_dtprofit': 'quarterly_deferred_tax_profit',       # 扣除非经常损益后的净利润(单季度)
    'q_eps': 'quarterly_earnings_per_share',             # 每股收益(单季度)
    'q_netprofit_margin': 'quarterly_net_profit_margin', # 销售净利率(单季度)
    'q_gsprofit_margin': 'quarterly_gross_profit_margin', # 销售毛利率(单季度)
    'q_exp_to_sales': 'quarterly_expense_to_sales',      # 销售期间费用率(单季度)
    'q_profit_to_gr': 'quarterly_profit_to_revenue',     # 净利润/营业总收入(单季度)
    'q_saleexp_to_gr': 'quarterly_sales_expense_to_revenue', # 销售费用/营业总收入(单季度)
    'q_adminexp_to_gr': 'quarterly_admin_expense_to_revenue', # 管理费用/营业总收入(单季度)
    'q_finaexp_to_gr': 'quarterly_finance_expense_to_revenue', # 财务费用/营业总收入(单季度)
    'q_impair_to_gr_ttm': 'quarterly_impairment_to_revenue_ttm', # 资产减值损失/营业总收入
    'q_gc_to_gr': 'quarterly_goods_cost_to_revenue',     # 营业成本/营业总收入(单季度)
    'q_op_to_gr': 'quarterly_operating_profit_to_revenue', # 营业利润/营业总收入(单季度)
    'q_roe': 'quarterly_return_on_equity',               # 净资产收益率(单季度)
    'q_dt_roe': 'quarterly_diluted_roe',                 # 稀释净资产收益率(单季度)
    'q_npta': 'quarterly_net_profit_to_assets',          # 总资产报酬率(单季度)
    'q_ocf_to_sales': 'quarterly_ocf_to_sales',          # 经营活动产生的现金流量净额/营业收入(单季度)
    'basic_eps_yoy': 'basic_eps_growth',                 # 基本每股收益同比增长率(%)
    'dt_eps_yoy': 'diluted_eps_growth',                  # 稀释每股收益同比增长率(%)
    'cfps_yoy': 'cash_flow_per_share_growth',            # 每股经营活动产生的现金流量净额同比增长率(%)
    'op_yoy': 'operating_profit_growth',                 # 营业利润同比增长率(%)
    'ebt_yoy': 'ebt_growth',                             # 利润总额同比增长率(%)
    'netprofit_yoy': 'earnings_growth',                  # 归属母公司股东的净利润同比增长率(%)
    'dt_netprofit_yoy': 'diluted_net_profit_growth',     # 归属母公司股东的净利润(扣除非经常损益)同比增长率(%)
    'ocf_yoy': 'operating_cash_flow_growth',             # 经营活动产生的现金流量净额同比增长率(%)
    'roe_yoy': 'roe_growth',                             # 净资产收益率(摊薄)同比增长率(%)
    'bps_yoy': 'book_value_growth',                      # 每股净资产同比增长率(%)
    'assets_yoy': 'total_assets_growth',                 # 资产总计同比增长率(%)
    'eqt_yoy': 'equity_growth',                          # 归属母公司的股东权益同比增长率(%)
    'tr_yoy': 'total_revenue_growth',                    # 营业总收入同比增长率(%)
    'or_yoy': 'revenue_growth',                          # 营业收入同比增长率(%)
    'q_gr_yoy': 'quarterly_revenue_growth',              # 营业总收入(单季度)同比增长率(%)
    'q_gr_qoq': 'quarterly_revenue_growth_qoq',          # 营业总收入(单季度)环比增长率(%)
    'q_sales_yoy': 'quarterly_sales_growth',             # 营业收入(单季度)同比增长率(%)
    'q_sales_qoq': 'quarterly_sales_growth_qoq',         # 营业收入(单季度)环比增长率(%)
    'q_op_yoy': 'quarterly_operating_profit_growth',     # 营业利润(单季度)同比增长率(%)
    'q_op_qoq': 'quarterly_operating_profit_growth_qoq', # 营业利润(单季度)环比增长率(%)
    'q_profit_yoy': 'quarterly_profit_growth',           # 净利润(单季度)同比增长率(%)
    'q_profit_qoq': 'quarterly_profit_growth_qoq',       # 净利润(单季度)环比增长率(%)
    'q_netprofit_yoy': 'quarterly_net_profit_growth',    # 归属母公司股东的净利润(单季度)同比增长率(%)
    'q_netprofit_qoq': 'quarterly_net_profit_growth_qoq', # 归属母公司股东的净利润(单季度)环比增长率(%)
    'equity_yoy': 'equity_growth_alt',                   # 净资产同比增长率
    'rd_exp': 'research_and_development_expense',        # 研发费用
    'update_flag': 'update_flag',                        # 更新标志
    
    # 每股指标
    'bps': 'book_value_per_share',                       # 每股净资产
    'ocfps': 'operating_cash_flow_per_share',            # 每股经营活动产生的现金流量净额
    'retainedps': 'retained_earnings_per_share',         # 每股留存收益
    'cfps': 'cash_flow_per_share',                       # 每股现金流量净额
    'ebit_ps': 'ebit_per_share',                         # 每股息税前利润
    'fcff_ps': 'free_cash_flow_per_share',               # 每股企业自由现金流量
    'fcfe_ps': 'free_cash_flow_to_equity_per_share',     # 每股股东自由现金流量
    
    # 盈利能力
    'netprofit_margin': 'net_margin',                    # 销售净利率(%)
    'grossprofit_margin': 'gross_margin',                # 销售毛利率(%)
    'cogs_of_sales': 'cost_of_goods_sold_ratio',         # 销售成本率(%)
    'expense_of_sales': 'expense_ratio',                 # 销售期间费用率(%)
    'profit_to_gr': 'profit_to_gross_revenue',           # 净利润/营业总收入(%)
    'saleexp_to_gr': 'sales_expense_to_revenue',         # 销售费用/营业总收入(%)
    'adminexp_to_gr': 'admin_expense_to_revenue',        # 管理费用/营业总收入(%)
    'finaexp_to_gr': 'finance_expense_to_revenue',       # 财务费用/营业总收入(%)
    'impair_to_gr_ttm': 'impairment_to_revenue_ttm',     # 资产减值损失/营业总收入(%)
    'gc_of_gr': 'goods_cost_ratio',                      # 营业成本/营业总收入(%)
    'ebitda': 'ebitda_margin',                           # EBITDA/营业总收入(%)
    
    # 资产质量指标
    'arturnover': 'accounts_receivable_turnover_alt',    # 应收账款周转率(次)
    'arturndays': 'accounts_receivable_turnover_days_alt', # 应收账款周转天数(天)
    'inventory_turnover': 'inventory_turnover_alt',      # 存货周转率(次)
    'inventory_days': 'inventory_turnover_days_alt',     # 存货周转天数(天)
    'currentasset_turnover': 'current_asset_turnover_alt', # 流动资产周转率(次)
    'currentasset_days': 'current_asset_turnover_days',  # 流动资产周转天数(天)
    
    # 杜邦分析
    'equity_multiplier': 'equity_multiplier',            # 权益乘数
    'roe_waa_2': 'roe_weighted_average_2',               # 净资产收益率_加权(%)
    'roe_avg': 'roe_average',                            # 净资产收益率_平均(%)
    'roe_waa_2_dedt': 'roe_weighted_average_2_deducted', # 净资产收益率_加权(扣除非经常损益)(%)
    'roe_avg_dedt': 'roe_average_deducted',              # 净资产收益率_平均(扣除非经常损益)(%)
    'roe_waa_2_nonr': 'roe_weighted_average_2_non_recurring', # 净资产收益率_加权(扣除非经常损益和股权激励费用)(%)
    'roe_avg_nonr': 'roe_average_non_recurring',         # 净资产收益率_平均(扣除非经常损益和股权激励费用)(%)
    'roe_waa_2_dedt_ttm': 'roe_weighted_average_2_deducted_ttm', # 净资产收益率_加权(扣除非经常损益)(TTM)(%)
    'roe_dt_2': 'roe_diluted_2',                         # 净资产收益率_摊薄(%)
    
    # 特殊计算指标
    'debt_to_equity_1': 'debt_to_equity_alt',            # 产权比率(另一种算法)
    'equity_ratio': 'equity_ratio',                      # 归属母公司股东权益/全部投入资本(%)
    'current_exint': 'current_excluding_interest',       # 流动比率(考虑货币时间价值)
    'non_current_exint': 'non_current_excluding_interest', # 非流动比率(考虑货币时间价值)
    'intrinsicvalue': 'intrinsic_value',                 # 内在价值
    'tmv': 'total_market_value',                         # 全部市值
    'lmv': 'liquid_market_value',                        # 流通市值
}

# 估值指标字段映射 (pro.daily_basic)
VALUATION_METRICS_FIELD_MAPPING = {
    'ts_code': 'ts_code',
    'trade_date': 'trade_date',
    'close': 'close_price',                              # 当日收盘价
    'turnover_rate': 'turnover_rate',                    # 换手率(%)
    'turnover_rate_f': 'turnover_rate_float',            # 换手率(自由流通股)
    'volume_ratio': 'volume_ratio',                      # 量比
    'pe': 'price_to_earnings_ratio',                     # 市盈率
    'pe_ttm': 'pe_ttm',                                  # 市盈率(TTM)
    'pb': 'price_to_book_ratio',                         # 市净率
    'ps': 'price_to_sales_ratio',                        # 市销率
    'ps_ttm': 'ps_ttm',                                  # 市销率(TTM)
    'dv_ratio': 'dividend_yield',                        # 股息率(%)
    'dv_ttm': 'dividend_yield_ttm',                      # 股息率(TTM)(%)
    'total_share': 'total_shares_outstanding',           # 总股本(万股)
    'float_share': 'outstanding_shares',                 # 流通股本(万股)
    'free_share': 'free_float_shares',                   # 自由流通股本(万股)
    'total_mv': 'total_market_value',                    # 总市值(万元)
    'circ_mv': 'circulating_market_value',               # 流通市值(万元)
}


def get_field_mapping(table_type: str) -> dict:
    """获取指定表类型的字段映射
    
    Args:
        table_type: 表类型 ('cashflow', 'income', 'balance', 'financial_metrics', 'valuation_metrics')
        
    Returns:
        dict: 字段映射字典
    """
    mappings = {
        'cashflow': CASHFLOW_FIELD_MAPPING,
        'income': INCOME_FIELD_MAPPING,
        'balance': BALANCE_FIELD_MAPPING,
        'financial_metrics': FINANCIAL_METRICS_FIELD_MAPPING,
        'valuation_metrics': VALUATION_METRICS_FIELD_MAPPING,
    }
    
    return mappings.get(table_type, {})


def get_tushare_fields(table_type: str, target_fields: list = None) -> str:
    """获取Tushare API需要的字段列表
    
    Args:
        table_type: 表类型 ('cashflow', 'income', 'balance', 'financial_metrics', 'valuation_metrics')
        target_fields: 目标字段列表，如果为None则返回所有字段
        
    Returns:
        str: 逗号分隔的Tushare字段名字符串
    """
    mapping = get_field_mapping(table_type)
    
    if target_fields:
        # 反向映射，从标准字段名找到Tushare字段名
        reverse_mapping = {v: k for k, v in mapping.items()}
        tushare_fields = []
        for field in target_fields:
            if field in reverse_mapping:
                tushare_fields.append(reverse_mapping[field])
        
        # 添加基础字段
        base_fields = ['ts_code', 'end_date', 'report_type', 'comp_type']
        for base_field in base_fields:
            if base_field not in tushare_fields:
                tushare_fields.insert(0, base_field)
        
        return ','.join(tushare_fields)
    else:
        # 返回所有字段
        return ','.join(mapping.keys())


def apply_field_mapping(data_dict: dict, table_type: str) -> dict:
    """应用字段映射，将Tushare字段名转换为标准字段名
    
    Args:
        data_dict: 包含Tushare字段的数据字典
        table_type: 表类型 ('cashflow', 'income', 'balance', 'financial_metrics', 'valuation_metrics')
        
    Returns:
        dict: 转换后的数据字典
    """
    mapping = get_field_mapping(table_type)
    result = {}
    
    for tushare_field, standard_field in mapping.items():
        if tushare_field in data_dict:
            result[standard_field] = data_dict[tushare_field]
    
    return result 