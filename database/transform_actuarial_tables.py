import pandas as pd
from numpy import nan as NA

pd.options.mode.chained_assignment = None  # default='warn'

yn_boolean_map = {'y': True, 'n': False, '': NA}
yn_converter = lambda x: yn_boolean_map[x.lower().strip()]
mf_boolean_map = {'m': True, 'f': False}
mf_converter = lambda x: mf_boolean_map[x.lower().strip()]
string_converter = lambda x: x.lower().strip() if x.strip() != '' else NA
billing_map = {'a': 1, 'm': 12, 'q': 4, 's': 2}
billing_converter = lambda x: billing_map[x.lower().strip()]
pay_type_converter = lambda x: True if x.strip().lower() == 'in' else False
float_converter = lambda x: round(float(x.strip()), 2)
int_converter = lambda x: int(x.strip())


def band_boundaries(x):
    boundaries = [0, ] + x['MaxFaceUnits'].to_list()
    minf = []
    maxf = []
    for i in range(len(x)):
        minf.append((boundaries[i] * 1000) + 1)
        maxf.append(boundaries[i + 1] * 1000)
    return pd.DataFrame({'LowerFaceAmount': minf, 'UpperFaceAmount': maxf}, index=x.index).astype(int)


# descriptions data
available_product_codes = ['ula20', 'ula20m', 'ull20', 'ull20m', 'w19gnc', 'w19mtc']
face_amount_minimums = {'ula20': 100000, 'ula20m': 100000,
                        'ull20': 100000, 'ull20m': 100000,
                        'w19gnc': 1000, 'w19mtc': 1000}
descriptions_fields = ['BandTable', 'CashValueTable', 'ModalProfileTable', 'PlanCode', 'PremiumTable',
                       'UnisexCashValues', 'UnisexRates']

descriptions = pd.read_csv('actuarial_tables/RateDescriptionTable.txt',
                           sep='\t',
                           usecols=descriptions_fields,
                           converters={'BandTable': string_converter,
                                       'CashValueTable': string_converter,
                                       'ModalProfileTable': string_converter,
                                       'PlanCode': string_converter,
                                       'PremiumTable': string_converter,
                                       'UnisexCashValues': yn_converter,
                                       'UnisexRates': yn_converter}) \
    .query("PlanCode in @available_product_codes") \
    .rename(columns={'ModalProfileTable': "ChargesTable"}) \
    .reset_index(drop=True)

descriptions['MinimumFaceAmount'] = descriptions['PlanCode'].map(face_amount_minimums)
descriptions = descriptions[sorted(descriptions.columns)]

# premium rates data
premium_tables = descriptions['PremiumTable'].unique()
premium_rates_fields = ['IssueAge', 'PremiumBand', 'PremiumPer1000', 'PremiumTable', 'RiskClass', 'Sex']
premium_rates = pd.read_csv('actuarial_tables/PremiumRateTable.txt',
                            sep='\t',
                            usecols=premium_rates_fields,
                            converters={'IssueAge': int_converter,
                                        'PremiumBand': int_converter,
                                        'PremiumPer1000': lambda x: round(float(x.strip()), 2),
                                        'PremiumTable': string_converter,
                                        'RiskClass': string_converter,
                                        'Sex': mf_converter}) \
    .query("PremiumTable in @premium_tables") \
    .rename(columns={"Sex": "MalePolicyGender"}) \
    .reset_index(drop=True)

# bands data
bands_fields = ['Band', 'BandTable', 'MaxFaceUnits']
bands_raw = pd.read_csv('actuarial_tables/BandTable.txt',
                        sep='\t',
                        usecols=bands_fields,
                        converters={'Band': int_converter,
                                    'BandTable': string_converter,
                                    'MaxFaceUnits': lambda x: float(x.strip())}) \
    .rename(columns={"Band": "PremiumBand"}) \
    .sort_values(by=['BandTable', 'PremiumBand']) \
    .reset_index(drop=True)

bands = pd.concat([bands_raw[['PremiumBand', 'BandTable']],
                   bands_raw.groupby('BandTable').apply(band_boundaries)], axis=1)
bands = bands[sorted(bands.columns)]

# policy charges data
charges_raw = pd.read_csv('actuarial_tables/ModalProfileTable.txt',
                          sep='\t',
                          converters={'BillingFrequency': billing_converter,
                                      'CollectionFee': float_converter,
                                      'ModalPolicyFee': float_converter,
                                      'ModalProfileTable': string_converter,
                                      'PaymentType': pay_type_converter,
                                      'PremiumFactor': float_converter}) \
    .rename(columns={'PaymentType': 'PaidByInvoice',
                     'ModalProfileTable': 'ChargesTable'})

charges = charges_raw[['ChargesTable', 'PaidByInvoice', 'BillingFrequency']]
charges['PolicyCharges'] = charges_raw[['PremiumFactor', 'ModalPolicyFee', 'CollectionFee']].sum(axis=1) * \
                           charges_raw["BillingFrequency"].round(2)
charges = charges[sorted(charges.columns)]

# cash values
cash_value_fields = ['CashValueTable', 'IssueAge', 'MaxPolicyYear', 'Sex', 'RiskClass', 'CashValuePer1000']
cash_values = pd.read_csv('actuarial_tables/CashValueRateTable.txt',
                          sep='\t',
                          usecols=cash_value_fields,
                          converters={'CashValuePer1000': float_converter,
                                      'CashValueTable': string_converter,
                                      'IssueAge': int_converter,
                                      'MaxPolicyYear': int_converter,
                                      'RiskClass': string_converter,
                                      'Sex': mf_converter}) \
    .rename(columns={"Sex": "MalePolicyGender"})

descriptions_table_fields = ['plan_code', 'premium_table', 'band_table', 'cash_value_table', 'charges_table',
                             'unisex_rates', 'unisex_cash_values', 'minimum_face_amount']
descriptions_rename = {'BandTable': 'band_table',
                       'CashValueTable': 'cash_value_table',
                       'ChargesTable': 'charges_table',
                       'MinimumFaceAmount': 'minimum_face_amount',
                       'PlanCode': 'plan_code',
                       'PremiumTable': 'premium_table',
                       'UnisexCashValues': 'unisex_cash_values',
                       'UnisexRates': 'unisex_rates'}
descriptions.rename(columns=descriptions_rename)[descriptions_table_fields] \
    .to_pickle("actuarial_tables/descriptions.pickle")

premium_rate_table_fields = ['premium_table', 'premium_band', 'issue_age', 'male_policy_gender', 'risk_class',
                             'premium_per_1000']
premium_rates_rename = {'PremiumTable': 'premium_table',
                        'IssueAge': 'issue_age',
                        'MalePolicyGender': 'male_policy_gender',
                        'RiskClass': 'risk_class',
                        'PremiumBand': 'premium_band',
                        'PremiumPer1000': 'premium_per_1000'}
premium_rates.rename(columns=premium_rates_rename)[premium_rate_table_fields] \
    .to_pickle("actuarial_tables/premium_rates.pickle")

bands_table_fields = ['band_table', 'premium_band', 'lower_face_amount', 'upper_face_amount']
bands_rename = {'BandTable': 'band_table',
                'LowerFaceAmount': 'lower_face_amount',
                'PremiumBand': 'premium_band',
                'UpperFaceAmount': 'upper_face_amount'}
bands.rename(columns=bands_rename)[bands_table_fields].to_pickle("actuarial_tables/bands.pickle")

charges_table_fields = ['charges_table', 'billing_frequency', 'paid_by_invoice', 'policy_charges']
charges_rename = {'BillingFrequency': 'billing_frequency',
                  'ChargesTable': 'charges_table',
                  'PaidByInvoice': 'paid_by_invoice',
                  'PolicyCharges': 'policy_charges'}
charges.rename(columns=charges_rename)[charges_table_fields].to_pickle("actuarial_tables/charges.pickle")

cash_value_table_fields = ['cash_value_table', 'issue_age', 'policy_year', 'male_policy_gender', 'risk_class',
                           'cash_value_per_1000']
cash_value_rename = {'CashValueTable': 'cash_value_table',
                     'IssueAge': 'issue_age',
                     'MaxPolicyYear': 'policy_year',
                     'MalePolicyGender': 'male_policy_gender',
                     'RiskClass': 'risk_class',
                     'CashValuePer1000': 'cash_value_per_1000'}
cash_values.rename(columns=cash_value_rename)[cash_value_table_fields].to_pickle("actuarial_tables/cash_values.pickle")
