#written for python v3.5.3
import logging, sys
import csv, os, itertools,time
import datetime as dt
import pandas as pd
import numpy as np
import Levenshtein as lev
import contextlib
import gc
from io import StringIO


sys.path.append('/Users/joynst01/Desktop/kungfu/05_library')
from myFunctions import gbDes, dfshape, agebarchart1, str2int, cntperct, group2table, linechart2, linechart, linechart3
from myFunctions import linechartFails, elementTW, dual_log

def levd(r,s1,s2):
    return lev.ratio(str(r['%s'%s1]).upper(),str(r['%s'%s2]).upper())


def fileDes(file,df,w,sn): #this will use the describe function and create a tab
    print('Describing file')
    fn = os.path.splitext(file)[0]
    df1List = df.columns.tolist()
    #print (df1List)
    cdf = pd.DataFrame(df1List)
    #print(cdf.head())
    qs1 = pd.DataFrame(df.describe().transpose())
    r = len(qs1.index) +5         
    #h = df.columns.tolist()
    print('Number of columns: %i'%len(df1List))
    nfile1 = fn[-30:]
    qs1.index.names=["columnNames"]
    if sn == "Y":
        qs1.to_excel(w, sheet_name=nfile1)
        ws = w.sheets[nfile1]
        ws.write(r,0,file)
    elif sn == "N":
        qs1.to_excel(w, sheet_name="fileDesc")
        ws = w.sheets["fileDesc"]
        ws.write(r,0,file)


def gbDes(h,df,w): # this will group by each column name and count
    print('\nGrouping by each column')
    hc = 0
    r = 0
    for l in h:
        hc = hc + 1
        col = pd.DataFrame(df.groupby(l)[l].count())
        col.columns = ['count']
        col.sort_values(by=['count'],ascending=False,inplace=True)
        col.reset_index(inplace=True)
        try:
            if r == 0:
                col.to_excel(w, sheet_name = "groubyCount",startrow =0,
                             startcol = 0, index=False)
                r = r +1+len(col.columns)
            else:
                col.to_excel(w, sheet_name = "groubyCount",startrow =0,
                             startcol = r, index=False)
                r = r + len(col.columns) + 1
        except Exception:
            pass
        if hc % 10 == 0:
            print('columns counted - %i'%hc)
    
def main():
    folder1 = "input"
    folder2 = "output"
    today = dt.datetime.strftime(dt.datetime.now(),'%Y%m%d')
    pd.set_option('display.max_columns',None)
    pd.options.mode.chained_assignment = None


    
    # setting the paths of the working directory 
    spath = "C:/SIUtemp/03_projects"
    epath = "/00_customers/2022/VA/VEC"
##    epath = "/00_customers/2022/FL/DOL/08_production"
##    epath = "/00_batch/2021/01helping/lori"
##    epath = "/00_customers/2021/FL/DCF/batch_testing"
##    epath = "/00_batch/2021/MD"

    
    print("current working dir \n",os.getcwd())
    os.chdir(spath+epath)
    print("changed working dir \n",os.getcwd())



    
    # user input for the type of options I added in and file extension
    test = "Y"
    if test == "Y":
        deep = "Y"
        ftype = "csv"
        samp = "N"
        dup = "N"
        dupSpecial = "N"
        addrscore = "N"
        tumbledemail = "N"

    else:
        deep = input('Detailed column summary Y/N:')
        ftype = input('What is the file extension (txt, csv, dat):')
        samp = input('Raw sample data in output/if N all data Y/N:')
        dup = input('Look for dups Y/N: ')

    def dfshape(df,text):
        row,col = df.shape
        print("%s - rows %s columns %s"%(text,row,col))

    if deep == 'Y':
        
        for file1 in os.listdir(folder1):

            chunksize = 100000
            df1 = pd.DataFrame()

            if file1 == "events_unk.csv":
            #if file1.lower().endswith(ftype):
                pfile1 = os.path.join(folder1,file1)
                fn = os.path.splitext(file1)[0]
                print(pfile1)


                dual_log(level=logging.ERROR,
                     format='%(asctime)s :: %(levelname)s :: %(message)s', force=True,
                     filename="output/debug_%s.log"%fn,
                     filemode='a')            
                with open(pfile1,'r') as f:
                    try:
                        dialect = csv.Sniffer().sniff(f.readline(), delimiters= ['|','\t',',',';','^'])
                        delim = dialect.delimiter
                        print(delim)
                        df1 = pd.read_csv(pfile1,sep=delim,dtype=str,encoding="ISO-8859-1", error_bad_lines=False,warn_bad_lines=True)
                        #"ISO-8859-1" "UTF-8"

                            
                    except Exception as e:
                        print(e)
                        print('made it here')
                        #with open(os.path.join(folder2,'log_%s_%s.txt')%(fn,today), 'w') as log:
                        #with contextlib.redirect_stderr(logging):
                        #sys.strerr = log
                        break
                        print("MISMATCHED COLUMNS")
                        m = 0
                        df3 = pd.DataFrame()
                        delim = input ('Delimiter used in file?: (| , \t)  ')
                        for cf in pd.read_fwf(pfile1, header=None, chunksize=chunksize, iterator=True):
                            df2 = cf[0].str.split(delim,expand=True)
                            df3 = df3.append(df2)
                            del cf
                            m = m + 1
                            if m % 10 == 0:
                                print('number loops - %i'%m)

                        #print(df3.shape)
                        #print(df3.head())

                        df3['isnull_cnt'] = df3.isnull().sum(axis=1)
                        df3 = pd.DataFrame(df3.loc[df3["isnull_cnt"] == 0])
                      
                        w2 = pd.ExcelWriter(os.path.join(folder2,fn+'_mismatched_%s.xlsx'%today), engine='xlsxwriter')
                        pd.ExcelWriter
                        df3.to_excel(w2, sheet_name="raw")
                        w2.save()
                        print('CHECK MISMATCHED FILE')
                        continue

                print('data loade complete')
                dfshape(df1,'dataframe')
                print(df1.head())
                
                    
                if samp == "Y":
                    w1 = pd.ExcelWriter(os.path.join(folder2,fn+'_SIUdescSample_%s.xlsx'%today), engine='xlsxwriter')
                    pd.ExcelWriter
                    
                    #df1.drop_duplicates(inplace=True)
                    
                    df1samp = df1.iloc[:1000,]
                    df1samp.to_excel(w1, sheet_name="rawSamp_data")
                else:
                    w1 = pd.ExcelWriter(os.path.join(folder2,fn+'_SIUdesc_%s.xlsx'%today), engine='xlsxwriter')
                    pd.ExcelWriter
##                    dfedrop = df1.dropna(axis='columns', how='all') 
                    df1.to_excel(w1, sheet_name="raw_data",index=False)

##                df1['best_citystate'] = df1['Best_city'].str.lower() + ' ' + df1['Best_state'].str.lower()
                
                print('Starting deep dive')
                df1 = df1.dropna(axis='columns', how='all') #only doing the group on populated columns
                h = df1.columns.tolist()
                fileDes(file1,df1,w1,"N")

                df1_coldrop = df1.dropna(axis='columns', how='all')
                h2 = df1_coldrop.columns.tolist()
                gbDes(h2,df1_coldrop,w1)

##                df1['is_dup'] = df1.duplicated(keep=False)
##                df1dp = pd.DataFrame(df1.loc[df1["is_dup"] == True])
##                df1dp.to_excel(w1, sheet_name="dups")
                
                # looking for dups based on a unique field, you can type it out or have it use the first field 
                if dup == "Y":
                    print('Finding dups')
##                    h1 = h[0]
                    h1 = "Account_Number"
                    df1["is_dup"] = df1.duplicated([h1],keep=False)
                    dfd = pd.DataFrame(df1.loc[df1["is_dup"] == True])
                    dfd.to_excel(w1, sheet_name="dupsby")
                    try:
                        df1["is_dup2"] = df1.duplicated(["did"],keep=False)
                        dfd2 = pd.DataFrame(df1.loc[df1["is_dup2"] == True])
                        dfd2.dropna(subset=['did'],inplace=True)
                        dfd2.to_excel(w1, sheet_name="dupsLexID")
                        qs2 = pd.DataFrame(dfd2.describe().transpose())
                        qs2.to_excel(w1, sheet_name="dupsLexID_desc")
                    except:
                        pass

                if dupSpecial == "Y":
                    print('Finding specfic dups')
                    dlist = ['DriversLicense', 'SSN', 'Phone','IPAddress','Email','BankAccountCurrent']
                    for d in dlist:
                        ddf = df1.loc[df1[d].notnull()]
                        ddf["is_dup"] = ddf.duplicated([d],keep=False)
                        dfd = pd.DataFrame(ddf.loc[ddf["is_dup"] == True])
                        dfd.to_excel(w1, sheet_name="dup_%s"%d)
                        del ddf
                        del dfd


                if tumbledemail == "Y":
                    df1["email_dup"] = df1.duplicated(["Email"],keep=False)
                    df1['period_count'] = df1['Email'].str.count('\.')
                    df1['tEmail'] = df1['Email'].str.replace('.', '',regex=True)
                    df1['tEmail_len'] = df1['tEmail'].str.len()
                    df1['temail_cnt'] = df1.groupby('tEmail')['tEmail'].transform('count')
                    df1.loc[(df1['temail_cnt']>1) & (df1['period_count']>1) & (df1["email_dup"]==False), "Tumbled"] ="Y"
                    df2 = df1.loc[df1['Tumbled']=='Y']
                    df2_drop = df2.dropna(axis='columns', how='all')
                    df2_drop.to_excel(w1, sheet_name="tumbemail",index=False)
                    
##                        df['period_count'] = df['Email'].str.count('\.')
##                        df1["email_dup"] = df1.duplicated(["Email"],keep=False)
##                        df1['tEmail'] = df1['Email'].str.replace('.', '', regex=True)
##                        df1["e2_dup"] = df1.duplicated(["tEmail"],keep=False)
##                        df1['temail_count'] = df1.groupby('tEmail')['tEmail'].transform('count')
##                        df2 = df1.loc[df1['temail_count']>1]
##                        df2_drop = df2.dropna(axis='columns', how='all')
##                        df2_drop.to_excel(w1, sheet_name="tumbemail",index=False)

                if addrscore =="Y":

                    rdptransRpt = 'N'
                    if rdptransRpt == 'Y':
                        print('\nworking on rdp transaction standard scores')
                        df1['in_fullname'] = df1['First Name'].str.lower() + ' ' + df1['Last Name'].str.lower()
                        df1['best_fullname'] =  df1['First Name.1'].str.lower() + ' ' + df1['Last Name.1'].str.lower()
                        df1['fullAddressIn'] =  df1['Street 1']+ ' ' +df1['Street 2'].fillna('')+ ' ' +df1['Suite'].fillna('')\
                                               + ' ' +df1['City']+ ' ' +  df1['State']
                        df1['fullAddressOut'] =  df1['Street 1.1']+ ' ' +df1['Street 2.1'].fillna('')+ ' ' +df1['Suite.1'].fillna('')\
                                                + ' ' +df1['City.1']+ ' ' +  df1['State.1']


                        
                        df1["name_in_best_score"]=df1.apply(levd,args=('in_fullname','best_fullname',),axis=1)
                        df1["addr_in_best_score"]=df1.apply(levd,args=('fullAddressIn','fullAddressOut',),axis=1)
                        df1["ssn_in_best_score"]=df1.apply(levd,args=('SSN','SSN.1',),axis=1)
                        df1["dob_in_best_score"]=df1.apply(levd,args=('DOB','DOB.1',),axis=1)

                        df1 = df1.dropna(axis='columns', how='all')
                        df1.to_excel(w1, sheet_name="scores",index=False)


                    

                    siuinternalADL = 'Y'
                    if siuinternalADL == 'Y':
                        print('\nworking on siu standard scores')
                        # if using the siu standard and adl output
                        df1['Ind_fullname'] = df1['Ind_First_Name'].str.lower() + ' ' + df1['Ind_Last_Name'].str.lower()
                        df1['best_fullname'] =  df1['best_fname'].str.lower() + ' ' + df1['best_lname'].str.lower()
                        df1['dec_fullname'] =  df1['dec_fname'].str.lower() + ' ' + df1['dec_lname'].str.lower()
                        df1['Ind_SSN'] = df1['Ind_SSN'].str.replace('-','')
                        # scoring
##                        df1["dob_in_best_score"]=df1.apply(levd,args=('Ind_DOB','best_dob',),axis=1)
##                        df1["dob_in_dec_score"]=df1.apply(levd,args=('Ind_DOB','dec_bdate',),axis=1)
                        df1["dob_best_dec_score"]=df1.apply(levd,args=('best_dob','dec_bdate',),axis=1)
                        df1["ssn_in_best_score"]=df1.apply(levd,args=('Ind_SSN','best_ssn',),axis=1)
                        df1["ssn_in_dec_score"]=df1.apply(levd,args=('Ind_SSN','dec_ssn',),axis=1)
                        df1["ssn_best_dec_score"]=df1.apply(levd,args=('best_ssn','dec_ssn',),axis=1)

                        df1["name_in_best_score"]=df1.apply(levd,args=('Ind_fullname','best_fullname',),axis=1)
                        df1["name_in_dec_score"]=df1.apply(levd,args=('Ind_fullname','dec_fullname',),axis=1)
                        df1["name_best_dec_score"]=df1.apply(levd,args=('best_fullname','dec_fullname',),axis=1)
                        
                        # removing the scores where no deceased information 
                        nlist = ['dec_fullname','dob_in_dec_score','dob_best_dec_score','name_in_dec_score','name_best_dec_score']
                        for n in nlist:
                            df1.loc[df1['dec_matchcode'].isnull(), n] = ''

                        df1 = df1.dropna(axis='columns', how='all')
                        
                        df1.to_excel(w1, sheet_name="scores",index=False)
                        


                    scdew_crim = 'N'
                    if scdew_crim == 'Y':
                        # SCDEW testing for federal crimnal history
                        df1['Ind_fullname'] = df1['FirstName'].str.lower() + ' ' + df1['LastName'].str.lower()
                        df1['crim_fullname'] =  df1['fname'].str.lower() + ' ' + df1['lname'].str.lower()


                        df1['SSN'] = df1['SSN'].str.replace('-', '')
                        df1['DOB'] = df1['DOB'].str.replace('-', '')
                        clist = ["DOB"]
                        for c in clist:
                            df1[c] = pd.to_datetime(df1[c], utc=True)
                            df1[c] = df1[c].dt.tz_localize(None)
                        df1['age'] = (dt.datetime.now()- df1["DOB"]).astype('<m8[Y]')
                        df1["DOB"] = df1["DOB"].dt.date
                        
                        # scoring
                        df1["dob_in_crim_score"]=df1.apply(levd,args=('DOB','dob_1',),axis=1)
                        df1["ssn_in_crim_score"]=df1.apply(levd,args=('SSN','ssn_1',),axis=1)
                        df1["name_in_crim_score"]=df1.apply(levd,args=('Ind_fullname','crim_fullname',),axis=1)

                        

##                        off_cols = [col for col in df1.columns if 'off_desc' in col]
                        off_cols = [col for col in df1 if col.startswith('off_desc')]
                        kcols = ['UniqueID','FirstName','LastName','SSN','DOB','age']

                        crimonly = df1[kcols + off_cols]
                        crimonly.to_excel(w1, sheet_name="crimonly", index=False)
                        df1.to_excel(w1, sheet_name="scores")

                    nycHRA = 'N'
                    if nycHRA =='Y':
                        df1['Ind_fullname'] = df1['FIRST_NAME'].str.lower() + ' ' + df1['LAST_NAME'].str.lower()
                        df1['Recip_fullname'] = df1['Recip_First_Name_RECPT'].str.lower() + ' ' + df1['Recip_Last_Name_RECPT'].str.lower()

                        df1['best_citystate'] = df1['Best_city'].str.lower() + ' ' + df1['Best_state'].str.lower()

                        df1["name_in_list1_score"]=df1.apply(levd,args=('Ind_fullname','phone_1_listing_name',),axis=1)
                        df1["name_in_list2_score"]=df1.apply(levd,args=('Ind_fullname','phone_2_listing_name',),axis=1)
                        df1["name_in_list3_score"]=df1.apply(levd,args=('Ind_fullname','phone_3_listing_name',),axis=1)

                        df1["name_rec_list1_score"]=df1.apply(levd,args=('Recip_fullname','phone_1_listing_name',),axis=1)
                        df1["name_rec_list2_score"]=df1.apply(levd,args=('Recip_fullname','phone_2_listing_name',),axis=1)
                        df1["name_rec_list3_score"]=df1.apply(levd,args=('Recip_fullname','phone_3_listing_name',),axis=1)


                        df1["phonein_list1_score"]=df1.apply(levd,args=('Phone_Number_CAD','phone_1_phone',),axis=1)
                        df1["phonein_list2_score"]=df1.apply(levd,args=('Phone_Number_CAD','phone_2_phone',),axis=1)
                        df1["phonein_list3_score"]=df1.apply(levd,args=('Phone_Number_CAD','phone_3_phone',),axis=1)
                        

                        df1.to_excel(w1, sheet_name="scores")
                        

                    nyctrsTestFiles = 'N'
                    if nyctrsTestFiles =='Y':
                        
                        ## NYC Teachers testing ##
                        df1["addr_score"]=df1.apply(levd,args=('TRS_Addr_Line1','LxsNxs_Bst_AdrLin1',),axis=1)
                        df1["city_score"]=df1.apply(levd,args=('TRS_Addr_City','LxsNxs_Bst_City',),axis=1)
                        df1["state_score"]=df1.apply(levd,args=('TRS_State','LxsNxs_Bst_State',),axis=1)
                        df1["zip_score"]=df1.apply(levd,args=('TRS_ZipCode','LxsNxs_Bst_ZipCode',),axis=1)
                        df1["zipext_score"]=df1.apply(levd,args=('TRS_ZipExt','LxsNxs_Bst_ZipExt',),axis=1)
                        

                    
##                    df1["addr_score"]=df1.apply(levd,args=('Ind_Address','best_addr1',),axis=1)
##                    df1["city_score"]=df1.apply(levd,args=('Ind_City','best_city',),axis=1)
##                    df1["state_score"]=df1.apply(levd,args=('Ind_State','best_state',),axis=1)
##                    df1["zip_score"]=df1.apply(levd,args=('Ind_Zip','best_zip',),axis=1)
##                    df1["lexid_score"]=df1.apply(levd,args=('Customer3','did',),axis=1)
                        df1.to_excel(w1, sheet_name="scores", index=False)

                    


##                    df1.to_excel(w1, sheet_name="scores")
                print('saving file')
                w1.save()
                
            else:
                print("Input files aren't useable for this script")
                

    # this is the not deep dive which means doesn't do the group by and count function only descriptions  
    elif deep == 'N':        
        print('Starting shallow dive')

        for file1 in os.listdir(folder1):
            fn = os.path.splitext(file1)[0]
            if samp == "Y":
                w1 = pd.ExcelWriter(os.path.join(folder2,fn+'_SIUdescSample_%s.xlsx'%today), dtype =str, engine='xlsxwriter') # creates an excel file that has the analysis in it
            else:
                w1 = pd.ExcelWriter(os.path.join(folder2,fn+'_SIUdes_%s.xlsx'%today), dtype = str, engine='xlsxwriter')
            pd.ExcelWriter
            if file1.lower().endswith(ftype):
                pfile1 = os.path.join(folder1,file1)
                print(pfile1)
                with open(pfile1,'r') as f:
                    try:
                        dialect = csv.Sniffer().sniff(f.readline(), delimiters= ['|','\t',',',';'])
                        delim = dialect.delimiter
                        df1 = pd.read_csv(pfile1,sep=delim,dtype=str,encoding="ISO-8859-1")
    ##                        df1 = pd.read_csv(pfile1,sep=delim,dtype=str,encoding="utf-8")
                        c = df1.columns.tolist()
                        print('Column headers')
                        print(c)
                    except:
                        print()
                        print("MISMATCHED COLUMNS")

                        delim = input ('Delimiter used in file?: (| , \t)  ')
                        df2 = pd.read_fwf(pfile1, header=None)
                        df3 = df2[0].str.split(delim,expand=True)
                        #print(df3.shape)
                        #print(df3.head())

                        df3['isnull_cnt'] = df3.isnull().sum(axis=1)
                        df3 = pd.DataFrame(df3.loc[df3["isnull_cnt"] == 0])
                      
                        w2 = pd.ExcelWriter(os.path.join(folder2,fn+'_mismatched_%s.xlsx'%today), engine='xlsxwriter')
                        pd.ExcelWriter
                        df3.to_excel(w2, sheet_name="raw")
                        w2.save()
                        print('CHECK MISMATCHED FILE')
                        continue

                if samp == "Y":
                    df1samp = df1.iloc[:1000,]
                    df1samp.to_excel(w1, sheet_name="rawSamp_data")
                else:
                    df1.to_excel(w1, sheet_name="raw_data",index=False)

                fileDes(file1,df1,w1,"N")

                w1.save()
                    


if __name__== "__main__":
    print('started script')
    starttime = dt.datetime.now()
       
    main()
    
    print('finished elapsed: %s'%(dt.datetime.now()-starttime))
##    input('hit enter to quit')
