#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from derivative import output
import time
import pp

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
          
          
class apply_func(object):
      def __init__(self,config,output_table,orderby_value,ascending_value,date_value,Progress,apply_type='loose'):
        self.config=config
        self.output_table=output_table
        self.orderby_value=orderby_value
        self.ascending_value=ascending_value
        self.date_value=date_value
        self.apply_type=apply_type
        self.Progress=Progress


      @decorator.timeit
      def main_apply(self):
        ppservers = ()
        job_server = pp.Server(ppservers=ppservers)
        print "Starting job with", job_server.get_ncpus(), "workers"
        final_result=pd.DataFrame()
        job_list=[]
        for tablename in self.config:
                  for period in self.config[tablename]:
                      for groupby in self.config[tablename][period]:
                          for var in self.config[tablename][period][groupby]:
                                  groupby_split=groupby.split(',')
                                  sql="select "+groupby+','+period+','+var+' from '+tablename
                                  details=(self.orderby_value,self.date_value,self.ascending_value)
                                  
                                  
                                  def main(sql,var,groupby_split,period,tablename,config,groupby,apply_type,details):
                                      import sys
                                      sys.path.append('/root/.ipython/profile_derivative/startup')
                                      from derivative import load_data,transform_period,function
                                      import pandas 
                                      print sql
                                      data=load_data(sql,contype='bigdata').load_data_main()
                                      trans_period=transform_period(data=data,period=period,groupby=groupby_split,var=var)
                                      trans_period.details=details
                                      data,data_dict=trans_period() 
                                      ascending=trans_period.ascending
                                      if var=='bankrepayrate':                                     #这里一定要记得改掉！！
                                         data[var][data[var]==99]=0
                                      final_result_temp=pandas.DataFrame()
                                      for stage in config[tablename][period][groupby][var]:
                                          if apply_type=='loose':
                                             split_stage=stage.split(':')
                                             if len(split_stage)==1:
                                                data_new=data.groupby(groupby_split,as_index=False,sort=False).nth(range(0,int(split_stage[0])))
                                             elif  len(split_stage)==2:
                                                beg,end=split_stage
                                                data_new=data.groupby(groupby_split,as_index=False,sort=False).nth(range(int(beg)-1,int(end)))
                                                            
                                          elif apply_type=='strict':
                                               split_stage=stage.split(':')
                                               if len(split_stage)==1:
                                                  data_new=data.query('index in %s'%range(1,int(split_stage[0])+1))
                                                                
                                               elif len(split_stage)==2:
                                                    beg,end=split_stage
                                                    data_new=data.query('index in %s'%range(int(beg),int(end)+1))
                                                
                                          print 'job of %s for period %s with %s shape start'%(var,stage,data.shape)
                                          group=data_new.groupby(groupby_split,as_index=False,sort=False) 
                                          result=group.min()[groupby_split]
                                          _stage=config[tablename][period][groupby][var][stage]
                                          if _stage.has_key('nor'):
                                             nor=_stage['nor']
                                             if nor:
                                                for func in nor:
                                                    if func=='cv':
                                                       result_temp=getattr(function,func)(group,var,groupby_split)
                                                    elif func in ('argmax','argmin'):
                                                       result_temp=getattr(function,func)(data_new,group,var,groupby_split)
                                                    elif func in ('first','last'):
                                                       result_temp=getattr(function,func)(group,ascending)
                                                    elif func in ('maxp','minp'):
                                                       result_temp=getattr(function,func)(data_dict,period,data_new,group,var,groupby_split)
                                                    else:
                                                       result_temp=getattr(function,func)(group)
                                                    result_temp.rename(columns = {var:var+'_%s_%s'%(stage.replace(':','to'),func)}, inplace = True)
                                                    result=pandas.merge(result,result_temp,on=groupby_split)
                                          if _stage.has_key('spe'):
                                             spe=_stage['spe']
                                             if spe:
                                                for func in spe:
                                                    for operator in spe[func]:
                                                          for value in spe[func][operator]:
                                                              if func in ('ls','fst'):
                                                                 result_temp=getattr(function,func)(data_new,var,groupby_split,str(value),operator,ascending)
                                                              elif func in ('lsv','fstv'):
                                                                 result_temp=getattr(function,func)(data_new,var,groupby_split,str(value),operator,ascending)
                                                              elif func in ('lsp','fstp'):
                                                                 result_temp=getattr(function,func)(data_dict,period,data_new,var,groupby_split,str(value),operator,ascending)                          
                                                              else:   
                                                                 result_temp=getattr(function,func)(data_new,group,var,groupby_split,str(value),operator)
                                                              result_temp.rename(columns = {var:var+'_%s_%s_%s_%s'%(stage.replace(':','to'),func,operator,str(value).replace('.','dot'))}, inplace = True)
                                                              result=pandas.merge(result,result_temp,on=groupby_split)
                                          if len(final_result_temp)==0:
                                              final_result_temp=pandas.concat([final_result_temp,result])
                                          else:
                                              final_result_temp=pandas.merge(final_result_temp,result,on=groupby_split)
                                      return final_result_temp,groupby_split
                                      
                                      
                                      
                                      
                                  job_list.append(job_server.submit(main,(sql,var,groupby_split,period,tablename,self.config,groupby,self.apply_type,details)))
        self.Progress.max=len(job_list)+1
        for job in job_list:
            var_result,groupby_split=job()
            self.Progress.value += 1
            if len(final_result)==0:
               final_result=pd.concat([final_result,var_result])
            else:
               final_result=pd.merge(final_result,var_result,on=groupby_split)
        out=output(final_result,self.output_table)
        out.output_main()
        self.Progress.value += 1

              