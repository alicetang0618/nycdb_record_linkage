import pandas as pd
import re
import csv
import difflib

def close_matches(x):
    tmp = difflib.get_close_matches(x, nycdb_df['clean_entity_name'])
    if len(tmp) != 0:
        return tmp[0]
    else:
        return -1

def select_match(row):
    rv = 0
    if (row['abbrev_name'] != -1 and row['matched_name'] in row['abbrev_name'])\
    or (row['partial_name'] != -1 and row['matched_name'] in row['partial_name']) \
    or row['match_rate'] >= 0.95 \
    or (row['match_rate'] >= 0.9 and row['partial_name'] == -1 and row['partial_name'] == -1):
        rv = row['matched_name']
    elif row['abbrev_name'] != -1 and row['partial_name'] != -1 and len(set(row['abbrev_name']).intersection(set(row['partial_name']))) == 1: 
        rv = set(row['abbrev_name']).intersection(set(row['partial_name']))[0]
    elif row['abbrev_name'] != -1 and len(row['abbrev_name']) == 1:
    	rv = row['abbrev_name'][0]
    elif row['partial_name'] != -1 and len(row['partial_name']) == 1 and (abs(len(row['clean_name'].split())-len(row['partial_name'][0].split())) <= 1 or len(row['clean_name']) < len(row['partial_name'][0])):
    	rv = row['partial_name'][0]
    elif row['match_rate'] >= 0.9:
        rv = row['matched_name']
    else:
        if row['abbrev_name'] == -1: row['abbrev_name'] = []
        if row['partial_name'] == -1: row['partial_name'] = []
        if row['matched_name'] == -1: row['matched_name'] = []
        else: row['matched_name'] = [row['matched_name']]
        for key in ['partial_name', 'matched_name', 'abbrev_name']:
            if type(row[key]) != list:
                row[key] = [row[key]]
        rv = row['matched_name'] + row['abbrev_name'] + row['partial_name']
    return rv

def standardize_string(x):
    x = re.sub(r'[^\w\s]', ' ', x.upper()).strip()
    rv = []
    for word in x.split():
        word = word.strip()
        if word[0].isdigit() and word[-1].isalpha():
            for i in range(len(word)):
                if word[i].isalpha():
                    rv += [word[:i], word[i:]]
                    break
        else: rv.append(word)
    rv_new = []
    for word in rv:
        if word in abbrevs:
            if len(abbrevs[word]) > 0 and abbrevs[word] not in rv_new:
                rv_new.append(abbrevs[word])
        elif word not in rv_new:
            rv_new.append(word)
    rv_new = " ".join(rv_new).replace(' N Y ', ' NY ').replace('AMBULANCE CORP','AMBULANCE CORPS').replace('CITY OF NEW YORK', 'NY').replace('NEW YORK','NY').replace('NEW YORK CITY', 'NY')
    return rv_new

def contain_numbers(s):
    for w in s.split():
        if w[0].isdigit():
            return True
    return False

def extract_numbers(s):
    num_list = []
    let_list = []
    for w in s.split():
        if w[0].isdigit(): num_list.append(w)
        else: let_list.append(w)
    return (num_list, let_list)

def is_abbrev(l1, l2):
    l1, l2 = order_list_by_len(l1, l2)
    has_abbrev = False
    for x in l1:
        if x != l2[0]:
            tmp = ""
            if len(l2) < len(x): return False
            for j in range(len(x)):
                tmp += l2[j][0]
            if tmp != x: return False
            else: has_abbrev = True
            if len(l2) > len(x): l2 = l2[len(x):]
            else: l2 = []
        elif l2.index(x) < len(l2) - 1: l2 = l2[l2.index(x)+1:]
        else: l2 = []
        if len(l2) == 0 and l1.index(x) != len(l1)-1: return False
    return (has_abbrev and len(l2) == 0)

def order_list_by_len(l1, l2):
    if len(l1) > len(l2):
        tmp = l1
        l1 = l2
        l2 = tmp
    return (l1, l2)

def list_contain(l1, l2):
    l1, l2 = order_list_by_len(l1, l2)
    for x in l1:
        if x not in l2: 
            return False
        elif l2.index(x) < len(l2) - 1:
            l2 = l2[l2.index(x)+1:]
        else:
            l2 = []
    return True

def partial_rate(l1, l2):
    cnt = 0
    l1, l2 = order_list_by_len(l1, l2)
    for x in l1:
        if x in l2: 
            cnt += 1
            if l2.index(x) < len(l2) - 1:
                l2 = l2[l2.index(x)+1:]
    return float(cnt)/len(l1)

def name_list_blocked(base_row):
        rv = []
        for j, nycdb_row in nycdb_num_df.iterrows():
            if base_row[0] == nycdb_row["split"][0]:
                rv.append(nycdb_row["clean_entity_name"])
        return rv

def num_matches(x, y):
    tmp = difflib.get_close_matches(x, y)
    if len(tmp) != 0:
        return tmp[0]
    else:
        return -1

def get_abbrev_name(name):
	if name in abbrev_names:
		if len(abbrev_names[name]) == 1:
			return abbrev_names[name][0]
		else:
			return abbrev_names[name]
	else:
		return -1

if __name__ == "__main__":

    nycdb_df = pd.read_csv("nycdb_large.csv")
    nycdb_df = nycdb_df[['Current Entity Name', 'Initial DOS Filing Date']]
    base_df = pd.read_csv("base.csv")
    good_match_df = pd.read_csv('good_matches.csv')

    ## Preprocessing

    abbrevs = {}
    for line in csv.reader(open("abbrevs.csv")):
        for i in range(len(line)):
            if not i == 0:
                abbrevs[line[i]] = line[0]

    nycdb_df['clean_entity_name'] = nycdb_df['Current Entity Name'].apply(standardize_string)
    base_df['clean_name'] = base_df['name'].apply(standardize_string)

    ## Check for duplicates

    #dup_nycdb_df = nycdb_df[nycdb_df.duplicated(['clean_entity_name']) | nycdb_df.duplicated(['clean_entity_name'], take_last=True)]
    nycdb_df['date'] = nycdb_df['Initial DOS Filing Date'].apply(lambda x: "-".join([x[6:], x[3:5], x[:2]]))
    nycdb_df = nycdb_df.sort(['clean_entity_name', 'date'])
    nycdb_df = nycdb_df.drop_duplicates(['clean_entity_name'])

    duplicated_df = base_df[base_df.duplicated(['clean_name']) | base_df.duplicated(['clean_name'], take_last=True)]
    # duplicated_df.to_csv('duplicated_df.csv')
    print "Original length: " + str(len(base_df))
    base_df = base_df.drop_duplicates(['clean_name'])
    print "After deduplication: " + str(len(base_df))

    ## 1. Simple Fuzzy Match
    df1 = nycdb_df.merge(base_df, left_on='clean_entity_name', right_on='clean_name', how='inner')
    base_df1 = base_df[~base_df['clean_name'].isin(df1['clean_name'])]
    print "df1: number of records matched: " + str(len(df1))
    print "Number of records left: " + str(len(base_df1))

    ## 2. Good matches
    good_match_df = good_match_df[good_match_df["Good match"]==1].drop(['Good match', 'Unnamed: 0', 'match_rate'], axis=1)
    good_match_df = good_match_df[~good_match_df['name'].isin(df1['name'])]
    print "gold matches: number of verified matches other than df1: " + str(len(good_match_df))
    base_df1 = base_df1[~base_df1['clean_name'].isin(good_match_df['clean_name'])]
    print "Number of records left: " + str(len(base_df1))

	## 3. Names containing numbers
    base_num_df = base_df1[base_df1['clean_name'].apply(contain_numbers)]
    base_df2 = base_df1[~base_df1['clean_name'].apply(contain_numbers)]
    nycdb_num_df = nycdb_df[nycdb_df['clean_entity_name'].apply(contain_numbers)]

    base_num_df["split"] = base_num_df['clean_name'].apply(extract_numbers)
    nycdb_num_df["split"]= nycdb_num_df['clean_entity_name'].apply(extract_numbers)
    base_num_df['num_matches'] = base_num_df["split"].apply(name_list_blocked)
	
    base_num_df['matched_name'] = base_num_df.apply(lambda row: num_matches(row['clean_name'], row['num_matches']), axis=1)
    df5 = nycdb_num_df.merge(base_num_df, left_on='clean_entity_name', right_on='matched_name', how='inner')
    base_num_df2 = base_num_df[base_num_df['matched_name']==-1]
    base_df2 = base_df2.append(base_num_df2.drop(['split',"num_matches","matched_name"], axis=1))
    print "df5: number of records matched: "+ str(len(df5))
    print "Number of records left: " + str(len(base_df2))

    ## 4. Abbreviations
    abbrev_names = {}
    for x in nycdb_df['clean_entity_name']:
        for y in base_df2['clean_name']:
            if is_abbrev(x.split(), y.split()):
                abbrev_names[y] = abbrev_names.get(y, [])
                abbrev_names[y].append(x)

    abbrev_names['SOS FOUNDATION'] = ['S O S FOUNDATION']
    abbrev_names['SAFE FOUNDATION'] = ['S A F E FOUNDATION']
    abbrev_names['WEST HELP'] = ['WEST H E L P']
    abbrev_names['BELL FOUNDATION']=['B E L L FOUNDATION']
    
    base_df2['abbrev_name'] = base_df2['clean_name'].apply(get_abbrev_name)

    #df2 = nycdb_df.merge(base_df2, left_on='clean_entity_name', right_on='abbrev_name', how='inner')
    #base_df3 = base_df2[~base_df2['abbrev_name'].isin(df2['abbrev_name'])]

    ## 5. Partial matches
    partial = {}
    for x in nycdb_df['clean_entity_name']:
        for y in base_df2['clean_name']:
            if list_contain(x.split(), y.split()):
                partial[y] = partial.get(y, [10000])
                sim_rate = abs(len(x)-len(y))
                if sim_rate < partial[y][0]: partial[y] = [sim_rate, x]
                elif sim_rate == partial[y][0]: partial[y].append(x)

    ## Adjusted observed mistakes:
    partial['YOU GOTTA BELIEVE OLDER CH ILD ADOPTION AND PERMANENCY MO'] = [1, 'YOU GOTTA BELIEVE OLDER CHILD ADOPTION PERMANENCY MOVEMENT']

    base_df2['partial_name'] = base_df2['clean_name'].apply(lambda x: partial[x][1:] if x in partial else -1)

    #df3 = nycdb_df3.merge(base_df3, left_on='clean_entity_name', right_on='partial_name', how='inner')

    ## 6. Put it all together
    ## To find the closest possible match in the NYC database according to match rate. 
    ## For those uncertain, generate a list of names that are probably the right match.

    base_df2["matched_name"] = base_df2["clean_name"].apply(close_matches)
    base_df2['match_rate'] = base_df2.apply(lambda x: difflib.SequenceMatcher(None, x['clean_name'], x['matched_name']).ratio() if x['matched_name']!=-1 else 0, axis=1)
    
    base_df2['suggested_names'] = base_df2.apply(lambda x: select_match(x), axis=1)

    base_df3 = base_df2[base_df2['suggested_names'].apply(lambda x: type(x)==str)]
    
    df4 = nycdb_df.merge(base_df3, left_on='clean_entity_name', right_on='suggested_names', how='inner')
    print "df4: number of records matched: "+str(len(df4))
    
    # result_df = df1.append(good_match_df)
    # result_df = result_df.append(df5.drop(["split_y","split_x","num_matches","matched_name"], axis=1), ignore_index=True)
    # result_df = result_df.append(df4.drop(["abbrev_name","partial_name","suggested_names"], axis=1), ignore_index=True)
    result_df = df1.append(good_match_df)
    result_df = result_df.append(df5[~df5['name'].isin(good_match_df['name'])].drop(["split_y","split_x","num_matches","matched_name"], axis=1), ignore_index=True)
    result_df = result_df.append(df4[~df4['name'].isin(good_match_df['name'])].drop(["abbrev_name","partial_name","suggested_names"], axis=1), ignore_index=True)

    print "result_df: total number of records matched: " + str(len(result_df))

    result_df = result_df.drop(['matched_name','date'], axis=1)
    result_df['match_rate'] = result_df.apply(lambda x: difflib.SequenceMatcher(None, x['clean_name'], x['clean_entity_name']).ratio(), axis=1)
    # result_df['match_rate'] = result_df.apply(lambda x: fuzz.ratio(x['clean_entity_name'], x['clean_name']), axis=1)
    # result_df['partial_rate'] = result_df.apply(lambda x: fuzz.partial_ratio(x['clean_entity_name'], x['clean_name']), axis=1)

    uncertain_df = base_df2[~base_df2['name'].isin(result_df['name'])]
    uncertain_df['suggested_names'] = uncertain_df['suggested_names'].apply(lambda x: [x] if type(x) == str else x)
    print "uncertain_df: total number of records not matched: "+ str(len(uncertain_df))

    ## dealing with duplicated ein and clean name
    dupein_df = result_df[result_df['ein'].apply(lambda x: x == x) & (result_df.duplicated(['ein']) | result_df.duplicated(['ein'], take_last=True))]
    uniein_df = result_df[~result_df['ein'].isin(dupein_df['ein'])]
    dedupein_df = dupein_df.sort(['ein', 'match_rate'], ascending = [0, 0]).drop_duplicates(['ein'])
    # result_df1 contains no duplicated ein or name
    result_df1 = dedupein_df.append(uniein_df).sort(['name'])
    print "result_df1: total number of records matched after getting rid of ein/name duplicates: " + str(len(result_df1))

    ## Summary:
    result_df.to_csv("results.csv")
    result_df1.to_csv("results_nodup.csv")
    uncertain_df[uncertain_df['suggested_names'].apply(lambda x: len(x) == 1)].to_csv("uncertain_new.csv")
    print("uncertain_df: number of records with an uncertain match: "+ str(len(uncertain_df[uncertain_df['suggested_names'].apply(lambda x: len(x) == 1)])))
    uncertain_df[uncertain_df['suggested_names'].apply(lambda x: len(x) > 1)].to_csv("multi_option_new.csv")
    print("uncertain_df: number of records with multiple matches: "+ str(len(uncertain_df[uncertain_df['suggested_names'].apply(lambda x: len(x) > 1)])))
    uncertain_df[uncertain_df['suggested_names'].apply(lambda x: len(x) == 0)].to_csv("no_match.csv")
    print("uncertain_df: number of records with no match found: "+ str(len(uncertain_df[uncertain_df['suggested_names'].apply(lambda x: len(x) == 0)])))
    duplicated_matches = result_df[result_df.duplicated(['clean_entity_name']) | result_df.duplicated(['clean_entity_name'], take_last=True)].sort("clean_entity_name")
    duplicated_matches.to_csv("duplicated_matches_new.csv")
    print("duplicated_df: number of duplicated matches: " + str(len(duplicated_matches)))