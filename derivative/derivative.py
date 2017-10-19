#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cStringIO
import MySQLdb
import psycopg2
import csv
import sys
import pandas as pd
import numpy as np
import logging
import datetime
import time
import yaml
import copy
import warnings
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')


class decorator(object):
    
      @staticmethod
      def timeit(func):
          def wrapper(*args, **kw):
              t1=time.time()
              result=func(*args, **kw)
              t2=time.time()
              print('function %s cost:%s s' %(func.__name__,str(round(t2-t1,3))))
              return result
          return wrapper     


class load_data(object):
      '''
      加载数据

      sql:加载数据的sql,必要参数。

      contype:加载方式:
              default(默认),即采用pd.read_sql_query方法，适用于列比较少的情况
              bigdata:执行完sql后在缓存中保存为csv,再调用pd.read_csv，适用于列比较多的情况
        
              100w行+3列   default:12s     bigdata:13s
              100w行+78列  default:921s    bigdata:567s
       
      connectid:连接id,默认为1(pg),请在basisdata.std_dbname_config里查看
      '''
        
      __slots__ = ('contype','sql','connectid')  
    
      def __init__(self,sql,contype='default',connectid=1):
          self.sql=sql
          self.contype=contype
          self.connectid=connectid

            
          if contype not in ('default','bigdata'):
             raise ValueError('''%s is not a contype,please choose 'default' or 'bigdata' '''%contype)
          
   
      def __repr__(self):
          return   '''
          Connect type: %s
          conncet id: %s
          sql:%s''' %(self.contype,self.connectid,self.sql)
        
      
      def default_conncet(self,sql,connectid,db):
          Y=db.DB_F(connectid).main() 
          Y.connect()
          result=Y.sqldataframe(sql)#return a dataframe
          Y.unconnect()
          return result
        
      def bigdata_connect(self,sql,connectid,db):
          Y=db.DB_F(connectid).main() 
          Y.connect()
          cursor=Y.cursor3     
          cursor.execute(sql)
          columns=[i[0] for i in cursor.description]
          s_buf = cStringIO.StringIO()
          csv_writer = csv.writer(s_buf,delimiter='\t')
          csv_writer.writerows(cursor)
          s_buf.seek(1)
          result=pd.read_csv(s_buf,delimiter='\t',engine='c',header=None)
          result.columns=columns
          s_buf.close()
          Y.unconnect()
          return result

      @decorator.timeit
      def load_data_main(self):
          sys.path.append('/root/etl/module')
          import db
          fuct_dict={'default':self.default_conncet,'bigdata':self.bigdata_connect}
          if self.sql:
             result=fuct_dict.get(self.contype)(self.sql,self.connectid,db)
             return result
        
        
        
class load_config(object):
      '''
      使用YAML来存储配置文件
      '''
      __slots__ = ('path','_spe_list','_nor_list') 
    
      def __init__(self,path):
          self.path=path
          self._spe_list=['cmgt','freq','ls','fst','cnsc','lsv','fstv','lsp','fstp']
          self._nor_list=['first','last','sum','avg','med','max','min','argmax','argmin','cv','maxp','minp']  
      
      @property
      def spe_list(self):
          return self._spe_list
        
      @spe_list.setter
      def spe_list(self,new_list):
          self._spe_list=new_list
        
      @property
      def nor_list(self):
          return self._nor_list
        
      @nor_list.setter
      def nor_list(self,new_list):
          self._nor_list=new_list    
        
        
      def load(self):
          with open(self.path,'r') as f:
               config=yaml.load(f)
          return config
      
      @classmethod
      def find(obj,path, tag):
          d=obj(path).load()  
          if tag in d:
             return  d[tag]
          for k, v in d.items():
              if isinstance(v, dict):
                  for i in find(v, tag):
                      return  i
      
      def check(self,x):
          if x.keys()[0] not in ['spe', 'nor']:
             return self.check(x[x.keys()[0]])
          else:
             if x.has_key('nor'):
                nor_check=[i for i in x['nor'] if i not in self._nor_list]
                if nor_check:
                   raise ValueError(''' 'nor' doesn't have type %s '''%(nor_check)) 

             if x.has_key('spe'):
                spe_check=[i for i in x['spe'] if i not in self._spe_list]
                if spe_check:
                   raise ValueError(''' 'spe' doesn't have type %s '''%(spe_check)) 
                for i in x['spe']:
                   operator_check=[j for j in x['spe'][i].keys() if j not in ('>','==','<','>=','<=')]
                   if operator_check:
                      raise ValueError('''%s  operator for func:%s is wrong '''%(operator_check,i)) 
                   for l in x['spe'][i]:
                       int_check=[y for y in x['spe'][i][l] if type(y) not in (int,float)]
                       if int_check:
                           raise ValueError('''%s for func:%s operator:%s isn't type int '''%(int_check,i,l))
    
      def __call__(self):
          self.check(self.load()) 
        
        
      @property 
      def show(self):
          load_dict=self.load()
          tree_str = yaml.dump(load_dict)
          tree_str = tree_str.replace("  ", " | ")
          print(tree_str) 
        
        
            
    
class transform_period(object):
      def __init__(self,**kw):
          for key in kw:
              setattr(self,key,kw[key])
          self._orderby='user'
          self._spec_date=None
          self._ascending=True
           

      @property
      def details(self):
          return 'orderby:%s   spec_date:%s  ascending:%s'%(self._orderby,self._spec_date,self._ascending)
    
      @property      
      def ascending(self):
          return self._ascending
        
      @details.setter
      def details(self,value):
          orderby,spec_date,ascending=value
          if orderby=='specific' and spec_date is None:
             raise ValueError('please enter a specific date')
          self._orderby=orderby
          self._spec_date=spec_date
          self._ascending=ascending
          

      def transform_user(self,data):
          if self._ascending:
             max_period=data.groupby(self.groupby)[self.period].max().reset_index()
             newname=self.period+'_max'
             max_period.rename(columns = {self.period:newname}, inplace = True)
             data_temp=pd.merge(data,max_period,how='left',on=(self.groupby))
             data_temp['stage']=(data_temp[newname]-data_temp[self.period]).apply(lambda x:(x.days/28)+1)

          else:
             min_period=data.groupby(self.groupby)[self.period].min().reset_index()
             newname=self.period+'_min'
             min_period.rename(columns = {self.period:newname}, inplace = True)
             data_temp=pd.merge(data,min_period,how='left',on=(self.groupby))
             data_temp['stage']=(data_temp[self.period]-data_temp[newname]).apply(lambda x:(x.days/28)+1)   
          
          data_dict=data_temp.copy()
          data_temp.drop([self.period,newname],1,inplace=True)
          groupby=copy.copy(self.groupby) 
          groupby.extend(['stage'])
          final=data_temp.groupby(groupby)[self.var].max().reset_index()
          final.set_index('stage',inplace=True)
          return final,data_dict  
        
      def transform_spec(self,data):
           data=data.copy()
           if self._spec_date:
             data['stage']=(pd.to_datetime(self._spec_date)-data[self.period]).apply(lambda x:(x.days/28)+1)
             data_dict=data.copy()
             data.drop([self.period],1,inplace=True)
             groupby=copy.copy(self.groupby) 
             groupby.extend(['stage'])
             final=data.groupby(groupby)[self.var].max().reset_index()
             final.set_index('stage',inplace=True)
             return final,data_dict
            
        
      def __call__(self):
          fuct_dict={'user':self.transform_user,'specific':self.transform_spec}
          
          if self.data[self.period].dtype!=int:
             self.data[self.period]=pd.to_datetime(self.data[self.period])
             return fuct_dict.get(self._orderby)(self.data)
          else:
             groupby=copy.copy(self.groupby)   #小问题，得用copy()
             groupby.extend([self.period])
             final=(self.data).groupby(groupby)[self.var].max().reset_index()
             final.rename(columns = {self.period:'stage'}, inplace = True)
             final.set_index('stage',inplace=True)
             data_dict=None
             return final,data_dict
            
       
        
class function(object):
      
    
      @staticmethod
      @decorator.timeit
      def sum(groupby):
          return groupby.sum()
        
      @staticmethod
      @decorator.timeit
      def avg(groupby):
          return groupby.mean() 
    
      @staticmethod
      @decorator.timeit
      def first(groupby,ascending):
          if ascending:
             return function.last(groupby,ascending=False) 
          return groupby.first()
        
      @staticmethod
      @decorator.timeit
      def last(groupby,ascending):
          if ascending:
             return function.first(groupby,ascending=False) 
          return groupby.last()
    
      @staticmethod
      @decorator.timeit
      def med(groupby):
          return groupby.median() 
        
      @staticmethod
      @decorator.timeit
      def max(groupby):
          return groupby.max() 
        
      @staticmethod
      @decorator.timeit
      def min(groupby):
          return groupby.min()         
        
      @staticmethod
      @decorator.timeit
      def argmax(data,groupby,var,groupby_split):
          columns=copy.copy(groupby_split)
          columns.append(var)  
          arg_df=pd.merge(data.reset_index(),groupby.max(),how='inner',on=(columns))
          df=arg_df.groupby(groupby_split)['stage'].min().reset_index()
          df.columns=columns
          return df
      
      @staticmethod
      @decorator.timeit
      def argmin(data,groupby,var,groupby_split):
          columns=copy.copy(groupby_split)
          columns.append(var)  
          arg_df=pd.merge(data.reset_index(),groupby.min(),how='inner',on=(columns))
          df=arg_df.groupby(groupby_split)['stage'].min().reset_index()
          df.columns=columns
          return df          
    

      @staticmethod
      @decorator.timeit
      def cv(grouped,var,groupby):
          columns=copy.copy(groupby)
          columns.append(var)  
          c_v=abs((grouped.std()/grouped.mean())[var].fillna(0))
          cv_df=pd.concat([grouped.max()[groupby],c_v],axis=1, ignore_index=True)
          cv_df.columns=columns
          return cv_df
      
      @staticmethod
      @decorator.timeit
      def cmgt(data,grouped,var,groupby,value,operator):
          condition=var+' '+operator+' '+value
          newdata=data.copy()
          newdata[var]=np.where(newdata.eval(condition),1,0)
          df=newdata.groupby(groupby).sum().reset_index()
          return df
    
      @staticmethod
      @decorator.timeit
      def freq(data,grouped,var,groupby,value,operator):
          condition=var+' '+operator+' '+value
          satisfy=data.query(condition).groupby(groupby).size()
          df=pd.merge(pd.DataFrame(grouped.size().rename('size')).reset_index(),pd.DataFrame(satisfy.rename('freq')).reset_index(),how='left',on=(groupby)).fillna(0)
          df.eval('%s=freq / size'%var)
          df.drop(['freq','size'],1,inplace=True)
          return df
        
      @staticmethod
      @decorator.timeit
      def ls(data,var,groupby,value,operator,ascending):
          if ascending:
             return function.fst(data,var,groupby,value,operator,ascending=False)
          else:  
             columns=copy.copy(groupby)
             columns.append(var)  
             condition=var+' '+operator+' '+value
             ls_df=pd.DataFrame(data.query(condition).reset_index().groupby(groupby)['stage'].max().rename(var)).reset_index() 
             stage_df=pd.DataFrame(data.reset_index().groupby(groupby)['stage'].max().rename('stage')).reset_index() 
             df=pd.merge(stage_df,ls_df,how='left',on=(groupby)).fillna(-1)
             df.drop(['stage'],1,inplace=True)
             return df
      
      @staticmethod
      @decorator.timeit
      def fst(data,var,groupby,value,operator,ascending):
          if ascending:
             return function.ls(data,var,groupby,value,operator,ascending=False)
          else:  
             columns=copy.copy(groupby)
             columns.append(var)  
             condition=var+' '+operator+' '+value
             fst_df=pd.DataFrame(data.query(condition).reset_index().groupby(groupby)['stage'].min().rename(var)).reset_index() 
             stage_df=pd.DataFrame(data.reset_index().groupby(groupby)['stage'].min().rename('stage')).reset_index() 
             df=pd.merge(stage_df,fst_df,how='left',on=(groupby)).fillna(-1)
             df.drop(['stage'],1,inplace=True)
             return df
            
      @staticmethod
      @decorator.timeit
      def cnsc(data,grouped,var,groupby,value,operator):
              columns=copy.copy(groupby)
              columns.append(var)
              columns2=copy.copy(groupby)
              columns2.append('continue_detail')
              condition=var+' '+operator+' '+value
              satisfy=data.query(condition).reset_index()
              satisfy['stage_2']=satisfy.groupby(groupby)['stage'].shift(1)
              satisfy['continue']=np.where((satisfy['stage']-satisfy['stage_2'])==1,1,0)
              satisfy['continue_detail']=1*(satisfy['continue']==0).cumsum()
              satisfy_temp=satisfy.groupby(columns2)['continue'].sum().reset_index()
              satisfy_final=satisfy_temp.groupby(groupby)['continue'].max().reset_index()
              df=pd.merge(grouped.max()[groupby],satisfy_final,how='left',on=(groupby)).fillna(0)
              df.columns=columns
              return df
              
      @staticmethod
      @decorator.timeit
      def lsv(data,var,groupby,value,operator,ascending=False): 
          if ascending:
             return function.fstv(data,var,groupby,value,operator,ascending=False)
          else:  
             columns=copy.copy(groupby)
             columns.append(var)
             columns2=copy.copy(groupby)
             columns2.append('stage')
             condition=var+' '+operator+' '+value
             ls_df=pd.DataFrame(data.query(condition).reset_index().groupby(groupby)['stage'].max().rename('stage')).reset_index() 
             stage_df=pd.merge(data.reset_index(),ls_df,how='inner',on=(columns2))[columns]
             grouped=data.groupby(groupby,as_index=False,sort=False)
             df=pd.merge(grouped.max()[groupby],stage_df,how='left',on=(groupby)).fillna(99999)
             df.columns=columns
             return df 
                
      @staticmethod
      @decorator.timeit
      def fstv(data,var,groupby,value,operator,ascending=False): 
          if ascending:
             return function.lsv(data,var,groupby,value,operator,ascending=False)
          else:  
             columns=copy.copy(groupby)
             columns.append(var)
             columns2=copy.copy(groupby)
             columns2.append('stage')
             condition=var+' '+operator+' '+value
             ls_df=pd.DataFrame(data.query(condition).reset_index().groupby(groupby)['stage'].min().rename('stage')).reset_index() 
             stage_df=pd.merge(data.reset_index(),ls_df,how='inner',on=(columns2))[columns]
             grouped=data.groupby(groupby,as_index=False,sort=False)
             df=pd.merge(grouped.max()[groupby],stage_df,how='left',on=(groupby)).fillna(99999)
             df.columns=columns
             return df     
        
        
      @staticmethod
      @decorator.timeit
      def maxp(data_dict,period,data,groupby,var,groupby_split):  
          argmax_df=function.argmax(data,groupby,var,groupby_split)
          columns=copy.copy(groupby_split)
          columns.append('stage')
          columns2=copy.copy(groupby_split)
          columns2.append(period)
          argmax_df.columns=columns  
          argmax_df_final=pd.merge(argmax_df,data_dict,how='inner',on=columns)[columns2]
          argmax_df_final[var]=(datetime.datetime.now()-argmax_df_final[period]).apply(lambda x:(x.days/28))
          argmax_df_final.drop([period],1,inplace=True)
          return argmax_df_final

      @staticmethod
      @decorator.timeit
      def minp(data_dict,period,data,groupby,var,groupby_split):  
          argmin_df=function.argmin(data,groupby,var,groupby_split)
          columns=copy.copy(groupby_split)
          columns.append('stage')
          columns2=copy.copy(groupby_split)
          columns2.append(period)
          argmin_df.columns=columns  
          argmin_df_final=pd.merge(argmin_df,data_dict,how='inner',on=columns)[columns2]
          argmin_df_final[var]=(datetime.datetime.now()-argmin_df_final[period]).apply(lambda x:(x.days/28))
          argmin_df_final.drop([period],1,inplace=True)
          return argmin_df_final        

      @staticmethod
      @decorator.timeit
      def lsp(data_dict,period,data,var,groupby,value,operator,ascending):  
          lsp_df=function.ls(data,var,groupby,value,operator,ascending)
          columns=copy.copy(groupby)
          columns.append('stage')
          columns2=copy.copy(groupby)
          columns2.append(period)
          lsp_df.columns=columns  
          lsp_df_final=pd.merge(lsp_df,data_dict,how='inner',on=columns)[columns2]
          lsp_df_final[var]=(datetime.datetime.now()-lsp_df_final[period]).apply(lambda x:(x.days/28))
          lsp_df_final.drop([period],1,inplace=True)
          grouped=data.groupby(groupby,as_index=False,sort=False)
          lsp_df_final=pd.merge(grouped.max()[groupby],lsp_df_final,how='left',on=(groupby)).fillna(-1)
          return lsp_df_final          
        
      @staticmethod
      @decorator.timeit
      def fstp(data_dict,period,data,var,groupby,value,operator,ascending):  
          fstp_df=function.fst(data,var,groupby,value,operator,ascending)
          columns=copy.copy(groupby)
          columns.append('stage')
          columns2=copy.copy(groupby)
          columns2.append(period)
          fstp_df.columns=columns  
          fstp_df_final=pd.merge(fstp_df,data_dict,how='inner',on=columns)[columns2]
          fstp_df_final[var]=(datetime.datetime.now()-fstp_df_final[period]).apply(lambda x:(x.days/28))
          fstp_df_final.drop([period],1,inplace=True)
          grouped=data.groupby(groupby,as_index=False,sort=False)
          fstp_df_final=pd.merge(grouped.max()[groupby],fstp_df_final,how='left',on=(groupby)).fillna(-1)
          return fstp_df_final              


class output(object):
      def __init__(self,data,table_name):
          self.data=data
          self.table_name=table_name
       
      @decorator.timeit
      def output_main(self):
          df=self.data.copy()
          engine=create_engine('postgresql://postgres:123456@10.253.44.6:5432/credit')
          df.head(1).to_sql(self.table_name,engine,if_exists='replace',schema='scorecard',index=False,chunksize=10000)
          sys.path.append('/root/etl/module')
          import db
          Y=db.DB_F(1).main()
          Y.connect()
          sql="truncate table scorecard.%s"%self.table_name
          Y.sqldelete(sql)
          Y.unconnect()
          Y.connect()
          cursor=Y.cursor3
          s_buf = cStringIO.StringIO()
          df.to_csv(s_buf,header=False,sep='\t',index=False)
          s_buf.seek(1)
          cursor.copy_from(s_buf,'scorecard.%s'%self.table_name,null='')
          Y.conn3.commit()
          Y.conn3.close()
          print 'success insert into database'
            