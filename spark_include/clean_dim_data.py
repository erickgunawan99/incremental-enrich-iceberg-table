import pandas as pd

df = pd.read_csv("county_demographics.csv")

# since can only find data at county level, need to aggregate to state level
int_cols = df.select_dtypes(include='int').columns
income_cols = [col for col in int_cols if col.startswith('Income')]
other_int_cols = [col for col in int_cols if col not in income_cols]

# take the mean for income columns, sum for the rest, not accurate at all but good enough for demo purposes
grouped_df = df.rename(columns={
    "State": "state_abbreviation"}, inplace=True)
grouped_df = df.groupby("state_abbreviation").agg({**{col: 'sum' for col in other_int_cols}, **{col: 'mean' for col in income_cols}}).reset_index()
pd.set_option('display.max_columns', None)
print(grouped_df.dtypes)
grouped_df.to_csv("us_states_dim1.csv", index=False)