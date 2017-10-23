# -*- coding: utf-8 -*-


"""

"""



import pp
import pandas 
import numpy 
import Weight_of_evidence
from Common_tools import Basesteps

__version__ = '0.0.2'


    
class Calculate_bin(object):

      @staticmethod
      def find_best_bin(series,target,min_bin,bin_diff,max_bin_number):
              _del_all=[]
              bin_edges=[0,1]
              max_iv=0
              per_pool=numpy.arange(min_bin,(1-min_bin)+bin_diff,bin_diff)
              per_pool=[round(i,3) for i in per_pool]
              best_bin_edges=best_bin_info=None
              print series.name
              if len(series.value_counts())>1:
                 while len(per_pool)>0:
                    for i in per_pool:
                        temp_edges=bin_edges[:]
                        temp_edges.insert(1,i)
                        temp_edges.sort()
                        print temp_edges
                        dis_var,bin_info=pandas.qcut(series,temp_edges,retbins=True,duplicates='drop') 
                        iv=Weight_of_evidence.Woe(dis_var,target).iv
                        if iv>max_iv:
                           max_iv=iv
                           best_bin_edges=temp_edges
                           best_bin_info=bin_info
                           best_i=i

                    if not best_bin_edges:
                       break

                    bin_number=len(best_bin_edges)-1
                    if (bin_edges==best_bin_edges) or (bin_number==max_bin_number):
                       break

                    _del=numpy.arange(best_i-min_bin,best_i+min_bin,bin_diff)
                    _del_all.extend([round(i,3) for i in  _del])  
                    per_pool=[i for i in per_pool if i not in _del_all]
                    bin_edges=best_bin_edges
              print 'best bin  for %s is Done'%(series.name)
              return best_bin_info,best_bin_edges

      @staticmethod
      def same_width(series,width_num):
          dis_var,bin_info=pandas.qcut(series,width_num,duplicates='drop',retbins=True)
          bin_edges=numpy.linspace(0,1,len(bin_info))
          return bin_info,bin_edges
        
class Bin(Basesteps):
       
      def __init__(self,data,
                   target,
                   min_bin=0.05,
                   bin_diff=0.01,
                   great_columns='ALL',
                   nor_columns=None,
                   width_num=None,
                   ignore_columns=None,max_bin_number=20):
         super(Bin,self).__init__(data,target,ignore_columns) 
         self.min_bin=min_bin
         self.bin_diff=bin_diff
         self.great_columns=great_columns
         self.nor_columns=nor_columns
         self.max_bin_number=max_bin_number
         self.width_num=width_num


      def bin_dict(self):        
          ppservers = ()
          job_server = pp.Server(ppservers=ppservers)
          print "Starting job with", job_server.get_ncpus(), "workers"
          _bin_dict={}
          job_dict={}
            
          def top_great_fuc(cls,*arg):
              return cls.find_best_bin(*arg)

          def top_nor_fuc(cls,*arg):
              return cls.same_width(*arg)  

          ins=Calculate_bin()
    
          great_name=self.get_columns(self.great_columns)
          nor_name=self.get_columns(self.nor_columns)

          if great_name is not None:
             for i in great_name:
                 job_dict[i]=job_server.submit(top_great_fuc,
                                            (ins,self.data[i],self.target,
                                             self.min_bin,self.bin_diff,
                                             self.max_bin_number),
                                             depfuncs=(),
                                             modules=('Weight_of_evidence','pandas','numpy'))
          if nor_name is not None:
             for i in nor_name:
                 job_dict[i]=job_server.submit(top_nor_fuc,
                                            (ins,self.data[i],self.width_num),
                                             depfuncs=(),
                                             modules=('pandas','numpy'))

          for col,job in job_dict.items():
              result=job()
              if result[0] is not None:
                 _bin_dict[col]=result
 
          return  _bin_dict
          

      def transform(self,_bin_dict):
          print self.ignore_columns
          dis_df=self.data[_bin_dict.keys()].apply(
                                lambda x:self.trans_func(x,_bin_dict[x.name][0]),
                                axis=0)
          if self.ignore_columns:
             dis_df=self.recover_func(dis_df)
          return dis_df

      def trans_func(self,series,bin):
          if series.max()>max(bin) or series.min()<min(bin):
             series[series>max(bin)]=max(bin)
             series[series<min(bin)]=min(bin)
          return numpy.digitize(series,bin,right=True)

          