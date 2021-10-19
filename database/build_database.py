import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column, Text, ForeignKey, Integer, PrimaryKeyConstraint, Float, Index, create_engine
)

metadata_obj = MetaData()

rate_description = Table('rate_description', metadata_obj,
                         Column('plan_code', Text(8), primary_key=True, unique=True),
                         Column('premium_table', Text(8), nullable=False),
                         Column('band_table', Text(8), nullable=False),
                         Column('cash_value_table', Text(8), nullable=True),
                         Column('charges_table', Text(2), nullable=False),
                         Column('unisex_rates', Integer, nullable=False),
                         Column('unisex_cash_values', Integer, nullable=True),
                         Column('minimum_face_amount', Integer, nullable=False),
                         Index('idx_pc', 'plan_code', unique=True))

band = Table('premium_band', metadata_obj,
             Column('band_table', Text(8), ForeignKey('rate_description.band_table'), nullable=False),
             Column('premium_band', Integer, nullable=False),
             Column('lower_face_amount', Integer, nullable=False),
             Column('upper_face_amount', Integer, nullable=False),
             PrimaryKeyConstraint('band_table', 'premium_band'),
             Index('idx_pt', 'band_table'))

premium_rate = Table('premium_rate', metadata_obj,
                     Column('premium_table', Text(8), ForeignKey('rate_description.premium_table'), nullable=False),
                     Column('premium_band', Integer, nullable=False),
                     Column('issue_age', Integer, nullable=False),
                     Column('male_policy_gender', Integer, nullable=False),
                     Column('risk_class', Text(1), nullable=False),
                     Column('premium_per_1000', Float, nullable=False),
                     PrimaryKeyConstraint('premium_table', 'premium_band', 'issue_age', 'male_policy_gender',
                                          'risk_class'),
                     Index('idx_nb', 'premium_table', 'issue_age', 'male_policy_gender', 'risk_class'),
                     Index('idx_b', 'premium_table', 'premium_band', 'issue_age', 'male_policy_gender', 'risk_class'))

policy_charge = Table('policy_charge', metadata_obj,
                      Column('charges_table', Text(2), ForeignKey('rate_description.charges_table'), nullable=False),
                      Column('billing_frequency', Integer, nullable=False),
                      Column('paid_by_invoice', Integer, nullable=False),
                      Column('policy_charges', Float, nullable=False),
                      PrimaryKeyConstraint('charges_table', 'billing_frequency', 'paid_by_invoice'),
                      Index('idx_c', 'charges_table', 'billing_frequency', 'paid_by_invoice'))

cash_value = Table('whole_life_cash_value', metadata_obj,
                   Column('cash_value_table', Text(8), ForeignKey('rate_description.cash_value_table'), nullable=False),
                   Column('issue_age', Integer, nullable=False),
                   Column('policy_year', Integer, nullable=False),
                   Column('male_policy_gender', Integer, nullable=False),
                   Column('risk_class', Text(1), nullable=False),
                   Column('cash_value_per_1000', Float, nullable=False),
                   PrimaryKeyConstraint('cash_value_table', 'issue_age', 'policy_year', 'male_policy_gender',
                                        'risk_class'),
                   Index('idx_wl', 'cash_value_table', 'issue_age', 'male_policy_gender', 'risk_class'))

descriptionsD = pd.read_pickle("actuarial_tables/descriptions.pickle")
premium_ratesD = pd.read_pickle("actuarial_tables/premium_rates.pickle")
bandsD = pd.read_pickle("actuarial_tables/bands.pickle")
chargesD = pd.read_pickle("actuarial_tables/charges.pickle")
cash_valuesD = pd.read_pickle("actuarial_tables/cash_values.pickle")

engine = create_engine("sqlite:///pricing.db", echo=True, future=True)
metadata_obj.create_all(engine)

descriptionsD.to_sql('rate_description', engine, if_exists='append', index=False)
premium_ratesD.to_sql('premium_rate', engine, if_exists='append', index=False)
bandsD.to_sql('premium_band', engine, if_exists='append', index=False)
chargesD.to_sql('policy_charge', engine, if_exists='append', index=False)
cash_valuesD.to_sql('whole_life_cash_value', engine, if_exists='append', index=False)

