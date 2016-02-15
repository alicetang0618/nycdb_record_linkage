from corp_match import * 

if __name__ == "__main__":

    ## 7362 rows
    base_df = pd.read_csv("base.csv")
    base_df['clean_name'] = base_df['name'].apply(standardize_string)
    base_df['name_length'] = base_df['name'].apply(lambda x: len(x))

    ## 878 rows have duplicated ein
    base_df[base_df['ein'].apply(lambda x: x == x) & (base_df.duplicated(['ein']) | base_df.duplicated(['ein'], take_last=True))]
    ## 1575 rows have a None value for ein
	base_df[base_df['ein'].apply(lambda x: x != x)]
	## 48 rows have duplicated name
	base_df[base_df.duplicated(['name']) | base_df.duplicated(['name'], take_last=True)]
	## 54 rows have duplicated clean_name
	base_df[base_df.duplicated(['clean_name']) | base_df.duplicated(['clean_name'], take_last=True)]

	## All together, 900 rows have duplicated eins, names, or clean_names.
	base_df[(base_df['ein'].apply(lambda x: x == x) \
		& (base_df.duplicated(['ein']) | base_df.duplicated(['ein'], take_last=True)))\
		| (base_df.duplicated(['name']) | base_df.duplicated(['name'], take_last=True))\
		| (base_df.duplicated(['clean_name']) | base_df.duplicated(['clean_name'], take_last=True))]

	## Drop those records exactly matched, including some duplicates
	## 4516 rows
	base_df1 = base_df[~base_df['clean_name'].isin(df1['clean_name'])]

	## Now 663 records have duplications
	base_df1[(base_df['ein'].apply(lambda x: x == x) \
		& (base_df.duplicated(['ein']) | base_df.duplicated(['ein'], take_last=True)))\
		| (base_df.duplicated(['name']) | base_df.duplicated(['name'], take_last=True))\
		| (base_df.duplicated(['clean_name']) | base_df.duplicated(['clean_name'], take_last=True))]

	## 250 duplicates were dropped + 185
	## 4082 rows left
	base_df2 = base_df1[base_df1['ein'].apply(lambda x: x == x)].sort(['ein', 'name_length'], ascending = [0, 0]).drop_duplicates(['ein'])
	base_df2 = base_df2[~base_df2['ein'].isin(df1['ein'])]
	base_df3 = base_df1[base_df1['ein'].apply(lambda x: x != x)].sort(['name_length'], ascending=[0]).drop_duplicates(['name']).drop_duplicates(['clean_name'])
	base_df4 = base_df2.append(base_df3)

	def fuzzy_match(x):
		rv = []
		for name in ['apple','banana','orange']:
			if x != name and (fuzz.ratio(x, name) >= 90 or fuzz.partial_ratio(x, name) >= 90 \
				or fuzz.token_sort_ratio(x, name) >= 90 or fuzz.token_set_ratio(x, name) >= 90): 	
				rv.append(name)
		return rv

	base_df4['match'] = base_df4['clean_name'].apply(fuzzy_match)
