import pandas as pd
import re
import csv

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
            if len(abbrevs[word]) > 0: rv_new.append(abbrevs[word])
        else:
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

if __name__ == "__main__":

    nycdb_df = pd.read_csv("nycdb_large.csv")
    nycdb_df = nycdb_df[['Current Entity Name', 'Initial DOS Filing Date']]
    base_df = pd.read_csv("base.csv")

    ## Preprocessing

    abbrevs = {}
    for line in csv.reader(open("abbrevs.csv")):
            for i in range(len(line)):
                    if not i == 0:
                            abbrevs[line[i]] = line[0]

    nycdb_df['clean_entity_name'] = nycdb_df['Current Entity Name'].apply(standardize_string)
    base_df['clean_name'] = base_df['name'].apply(standardize_string)

    ## Check for duplicates

    ## 163 rows in the nyc database have duplicated names. Only keep those with the earliest incorporate date.
    #dup_nycdb_df = nycdb_df[nycdb_df.duplicated(['clean_entity_name']) | nycdb_df.duplicated(['clean_entity_name'], take_last=True)]
    nycdb_df['date'] = nycdb_df['Initial DOS Filing Date'].apply(lambda x: "-".join([x[6:], x[3:5], x[:2]]))
    nycdb_df = nycdb_df.sort(['clean_entity_name', 'date'])
    nycdb_df = nycdb_df.drop_duplicates(['clean_entity_name'])

    #base_df[base_df.duplicated(['name']) | base_df.duplicated(['name'], take_last=True)]
    ## 54 rows in the nonprofit database share its name with another row.
    ## ein and ID.org are not unique: e.g. (112478910, C8, 68TH PRECINCT YOUTH COUNCIL) appears twice.
    ## Since we only match on the organization names, drop the duplicated 27 rows for now. 7335 rows left.
    duplicated_df = base_df[base_df.duplicated(['clean_name']) | base_df.duplicated(['clean_name'], take_last=True)]
    base_df = base_df.drop_duplicates(['clean_name'])


    ## 1. Simple Fuzzy Match: 2830 rows were matched.
    df1 = nycdb_df.merge(base_df, left_on='clean_entity_name', right_on='clean_name', how='inner')
    nycdb_df1 = nycdb_df[~nycdb_df.clean_entity_name.isin(df1['clean_entity_name'])]
    base_df1 = base_df[~base_df['clean_name'].isin(df1['clean_name'])]


    ## 2. Names containing numbers: 200 rows contain numbers.
    base_num_df = base_df1[base_df1['clean_name'].apply(contain_numbers)]
    base_df2 = base_df1[~base_df1['clean_name'].apply(contain_numbers)]
    nycdb_num_df = nycdb_df1[nycdb_df1['clean_entity_name'].apply(contain_numbers)]
    # nycdb_df2 = nycdb_df1[~nycdb_df1['clean_entity_name'].apply(contain_numbers)]

    base_num_df["split"] = base_num_df['clean_name'].apply(extract_numbers)
    nycdb_num_df["split"]= nycdb_num_df['clean_entity_name'].apply(extract_numbers)

    base_num_df['num_matches'] = base_num_df["split"].apply(name_list_blocked)

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

    # 91 rows were matched.
    base_num_df['matched_name'] = base_num_df.apply(lambda row: num_matches(row['clean_name'], row['num_matches']), axis=1)
    df5 = nycdb_num_df.merge(base_num_df, left_on='clean_entity_name', right_on='matched_name', how='inner')
    ## 109 records cannot be matched if the numbers exactly match.
    base_num_df2 = base_num_df[base_num_df['matched_name']==-1]
    base_df2 = base_df2.append(base_num_df2.drop(['split',"num_matches","matched_name"], axis=1))

    ## 3. Abbreviations
    abbrev_names = {}
    for x in nycdb_df['clean_entity_name']:
        for y in base_df2['clean_name']:
            if is_abbrev(x.split(), y.split()):
                abbrev_names[y] = abbrev_names.get(y, [])
                abbrev_names[y].append(x)
    # dup_abbrevs = {}
    # for i in abbrev_names:
    #     if len(abbrev_names[i])>1:
    #         dup_abbrevs[i] = abbrev_names[i]

    abbrev_names['SOS FOUNDATION'] = ['S O S FOUNDATION']
    abbrev_names['SAFE FOUNDATION'] = ['S A F E FOUNDATION']
    abbrev_names['WEST HELP'] = ['WEST H E L P']
    abbrev_names['BELL FOUNDATION']=['B E L L FOUNDATION']

    #base_df2['abbrev_name'] = base_df2['clean_name'].apply(lambda x: abbrev_names[x][0] if x in abbrev_names 
    #    and len(abbrev_names[x]) == 1 and abbrev_names[x][0].replace(" ", "") == x.replace(" ", "") else -1)
    base_df2['abbrev_name'] = base_df2['clean_name'].apply(lambda x: abbrev_names[x][0] if x in abbrev_names 
        and len(abbrev_names[x]) == 1 else -1)

    ## 105 rows were matched. Need to adjust for 4 duplicated matches.
    df2 = nycdb_df.merge(base_df2, left_on='clean_entity_name', right_on='abbrev_name', how='inner')
    nycdb_df3 = nycdb_df1[~nycdb_df1.clean_entity_name.isin(df2['clean_entity_name'])]
    base_df3 = base_df2[~base_df2['abbrev_name'].isin(df2['abbrev_name'])]


    ## 4. Partial
    partial = {}
    for x in nycdb_df['clean_entity_name']:
        for y in base_df3['clean_name']:
            if list_contain(x.split(), y.split()):
                partial[y] = partial.get(y, [10000])
                sim_rate = abs(len(x)-len(y))
                if sim_rate < partial[y][0]: partial[y] = [sim_rate, x]
                elif sim_rate == partial[y][0]: partial[y].append(x)
    # dup_partial = {}
    # for i in partial:
    #     if len(partial[i]) > 1:
    #         dup_partial[i] = partial[i]

    ## Adjusted observed mistakes:
    partial['YOU GOTTA BELIEVE OLDER CH ILD ADOPTION AND PERMANENCY MO'] = [1, 'YOU GOTTA BELIEVE OLDER CHILD ADOPTION PERMANENCY MOVEMENT']

    base_df3['partial_name'] = base_df3['clean_name'].apply(lambda x: partial[x][1] if x in partial 
        and len(partial[x]) == 2 and (abs(len(x.split())-len(partial[x][1].split())) == 1 or len(x) < len(partial[x][1])) else -1)

    ## 878 rows were matched.
    df3 = nycdb_df3.merge(base_df3, left_on='clean_entity_name', right_on='partial_name', how='inner')
    
    result_df = df1.append(df2.drop(["abbrev_name"], axis=1), ignore_index=True)
    result_df = result_df.append(df5.drop(["split_y","split_x","num_matches","matched_name"], axis=1), ignore_index=True)
    result_df = result_df.append(df3.drop(["abbrev_name","partial_name"], axis=1), ignore_index=True)
    
    nycdb_df4 = nycdb_df3[~nycdb_df3.clean_entity_name.isin(df3['clean_entity_name'])]
    base_df4 = base_df3[~base_df3['partial_name'].isin(df3['partial_name'])]

    ## PROBLEM: 97 rows have a match duplicated with one or more rows. See "partial_list.csv".
    dup_partial_matches = df3[df3.duplicated('clean_entity_name')|df3.duplicated('clean_entity_name', take_last=True)].sort("clean_entity_name")
    dup_partial_matches.to_csv("partial_list.csv")
    
    base_df4.to_csv("base_df4.csv")
    nycdb_df.to_csv("nycdb_df.csv")


    ## 5. Put it all together
    ## To find the closest possible match in the NYC database according to match rate. 
    ## For those uncertain, generate a list of names that are probably the right match.

    import difflib
    def close_matches(x):
        tmp = difflib.get_close_matches(x, nycdb_df['clean_entity_name'])
        if len(tmp) != 0:
            return tmp[0]
        else:
            return x

    base_df1["matched_name"] = base_df1["clean_name"].apply(close_matches)
    base_df4['match_rate'] = base_df4.apply(lambda x: difflib.SequenceMatcher(None, x['clean_name'], x['matched_name']).ratio(), axis=1)
    base_df4['partial_name'] = base_df4['clean_name'].apply(lambda x: partial[x][1:] if x in partial else -1)
    base_df4['abbrev_name'] = base_df4['clean_name'].apply(lambda x: abbrev_names[x] if x in abbrev_names else -1)

    # for index, row in base_df4.iterrows():
    #     if row['match_rate'] < 0.9:
    #         if row['partial_name'] != -1 and len(row['partial_name']) > 1:
    #             row['matched_name'] = row['partial_name']
    #             row['match_rate'] = partial_rate
    #         elif row['abbrev_name'] != -1:
    #             partial_rate = fuzz.partial_ratio(row['clean_name'], row['abbrev_name'])
    #             if partial_rate > row['match_rate']: 
    #                 row['matched_name'] = row['abbrev_name']
    #                 row['match_rate'] = partial_rate
    
    base_df4['suggested_names'] = base_df4.apply(lambda x: select_match(x), axis=1)
    
    def select_match(row):
        rv = 0
        if (row['abbrev_name'] != -1 and row['matched_name'] in row['abbrev_name'])\
        or (row['partial_name'] != -1 and row['matched_name'] in row['partial_name']) \
        or (row['match_rate'] >= 0.95 and row['match_rate'] != 1 and row['partial_name'] == -1 and row['partial_name'] == -1):
            rv = row['matched_name']
        elif row['abbrev_name'] != -1 and row['partial_name'] != -1 and len(set(row['abbrev_name']).intersection(set(row['partial_name']))) == 1: 
            rv = set(row['abbrev_name']).intersection(set(row['partial_name']))[0]
        else:
            if row['abbrev_name'] == -1: row['abbrev_name'] = []
            if row['partial_name'] == -1: row['partial_name'] = []
            if row['match_rate'] == 1: row['matched_name'] = []
            else: row['matched_name'] = [row['matched_name']]
            rv = row['matched_name'] + row['abbrev_name'] + row['partial_name']
        return rv

    base_df5 = base_df4[base_df4['suggested_names'].apply(lambda x: type(x)==str)]
    base_df6 = base_df4[base_df4['suggested_names'].apply(lambda x: type(x)==list)]
    base_df6[base_df6['match_rate'].apply(lambda x: x >= 0.9 and x != 1)]
    ## 441 rows were matched.
    df4 = nycdb_df4.merge(base_df5, left_on='clean_entity_name', right_on='suggested_names', how='inner')
    result_df = result_df.append(df4.drop(["abbrev_name","partial_name","suggested_names"], axis=1), ignore_index=True)

    ## Other results that can be kept after inspections on the output: Keep those with a match rate >= 0.9.
    base_df7 = base_df6[base_df6['match_rate'].apply(lambda x: x >= 0.9 and x != 1) & base_df6['clean_name'].apply(lambda x: x!='ITALIC INSTITUTE OF AMERICA')]
    base_df8 = base_df6[~base_df6['clean_name'].isin(base_df7['clean_name'])]
    ## 260 rows were matched.
    df6 = nycdb_df4.merge(base_df7, left_on='clean_entity_name', right_on='matched_name', how='inner')
    result_df = result_df.append(df6.drop(["abbrev_name","partial_name","suggested_names"], axis=1), ignore_index=True)

    uncertain_df = base_df8.append(base_num_df2.drop(["split","num_matches","matched_name"], axis=1))
    uncertain_df = uncertain_df.drop(['partial_name', 'abbrev_name','matched_name','match_rate'],axis=1)

    result_df = result_df.drop(['matched_name','date'], axis=1)
    result_df['match_rate'] = result_df.apply(lambda x: fuzz.ratio(x['clean_entity_name'], x['clean_name']), axis=1)
    result_df['partial_rate'] = result_df.apply(lambda x: fuzz.partial_ratio(x['clean_entity_name'], x['clean_name']), axis=1)

    ## Summary:
    ## 4480 records were matched. Errors exist, especially for the duplicated matches.
    result_df.to_csv("results.csv")
    ## 408 records need manual selection among suggestions
    uncertain_df[uncertain_df['suggested_names'].apply(lambda x: len(x) > 1 if type(x) == list else False)].to_csv("multi_options.csv")
    ## 2447 records cannot be matched. The function didn't find a name close enough.
    uncertain_df[~uncertain_df['suggested_names'].apply(lambda x: len(x) > 1 if type(x) == list else False)].to_csv("uncertain_records.csv")
    ## 147 duplicated matches.  
    duplicated_matches = result_df[result_df.duplicated(['clean_entity_name']) | result_df.duplicated(['clean_entity_name'], take_last=True)].sort("clean_entity_name")
    duplicated_matches.to_csv("duplicated_matches.csv")


    ## Second pass

    def close_matches(x):
        tmp = difflib.get_close_matches(x, nycdb_df['clean_entity_name'])
        if len(tmp) != 0:
            return tmp[0]
        else:
            return -1

    uncertain_df['clean_name_new'] = uncertain_df['clean_name'].apply(lambda x: x.replace("LIMITED", "LTD")\
        .replace("INC", "").replace("CENTRE", "CENTER"))
    uncertain_df["matched_name"] = uncertain_df["clean_name_new"].apply(close_matches)