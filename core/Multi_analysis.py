# -*- coding: utf-8 -*-


"""

"""

import pandas as pd 
import numpy as np 
from Model import Models
import matplotlib.pyplot as plt
from Common_tools import Basesteps


__version__ = '0.0.2'


class Multiple(Basesteps):

      def __init__(self,data,
                       target,
                       ignore_columns=None,
                       class_weight='balanced'):
          super(Multiple,self).__init__(data,target,ignore_columns)
          self.class_weight=class_weight
          self._corr_ratio=0.6

      @property
      def corrMat(self):
          return self.data.astype('float64').corr()


      @property
      def corr_group(self):
          corrMatrix=self.data.astype('float64').corr()
          corrMatrix.loc[:,:] =  np.tril(corrMatrix, k=-1) 
          already_in = set()
          result = []
          for col in corrMatrix:
              perfect_corr = corrMatrix[col][abs(corrMatrix[col])>self._corr_ratio].index.tolist()
              if perfect_corr and col not in already_in:
                 sec_temp=[i for i in perfect_corr if i in already_in]
                 if sec_temp:
                    sec=corrMatrix[col].loc[sec_temp].idxmax()
                    for l in range(0,len(result)):
                        if sec in result[l]:
                           result[l].append(col)
                           already_in.update(col)
                 else:
                     already_in.update(set(perfect_corr))
                     perfect_corr.append(col)
                     result.append(perfect_corr)
          return result


      @property
      def corr_ratio(self):
          return  self._corr_ratio


      @corr_ratio.setter
      def corr_ratio(self, corr_ratio):
          self._corr_ratio = corr_ratio


      def corr_reduce(self,uni_table):
          group_list=self.corr_group
          while group_list:
                group_list_iv=uni_table.loc[group_list[0]]['Iv']
                drop_colums=group_list_iv.drop(group_list_iv.idxmax()).index.values
                self.data.drop(drop_colums,1,inplace=True)
                group_list=self.corr_group
          ret_data=self.data.copy()
          if self.ignore_columns:      
             ret_data=self.recover_func(ret_data)
          return ret_data

      def __sorted(self,model):
          return sorted(zip(map(lambda x: round(x, 4),model), self.data.columns))


      def RFE(self,ins,n_features_to_select):
          rfe_model=ins.rfe(n_features_to_select=n_features_to_select)
          return self.__sorted(rfe_model.ranking_)


      def RFECV(self,ins,n_KFold,cv_scoring):
          rfecv_model=ins.rfecv(n_KFold=n_KFold,cv_scoring=cv_scoring)
          plt.xlabel("Number of features selected")
          plt.ylabel("Scoring: %s"%(cv_scoring))
          plt.plot(range(1, len(rfecv_model.grid_scores_) + 1), rfecv_model.grid_scores_)
          return self.__sorted(rfecv_model.ranking_)


      def RL(self,ins,sample_fraction,selection_threshold):
          rlr_model=ins.randlogistic(selection_threshold=selection_threshold,
                                     sample_fraction=sample_fraction)
          return self.__sorted(rlr_model.scores_)


      def _make_dataframe(self,value,col):
          return pd.DataFrame(value,columns=[col,'columns'])


      def multi_analysis_table(self,n_features_to_select,
                                    n_KFold=3,
                                    cv_scoring='roc_auc',
                                    rl=True,
                                    sample_fraction=0.25,selection_threshold=0.75):
          ins=Models(self.data,self.target)
          rfe_rank=self._make_dataframe(self.RFE(ins,n_features_to_select),'rfe')
          print 'rfe success'
          rfecv_rank=self._make_dataframe(self.RFECV(ins,n_KFold,cv_scoring),'rfecv')
          print 'rfecv success'
          result_list=[rfe_rank,rfecv_rank]
          if rl:
             rl_rank=self._make_dataframe(self.RL(ins,sample_fraction,selection_threshold),'rl')
             result_list.append(rl_rank)
             print 'rl success'
          result_table=pd.concat(result_list,axis=1)
          return result_table


      def multi_reduce(self,result_table):
          spl=len(result_table.columns)
          stay_list=[]
          stay_set=set()
          for i in range(0,spl,2):
              temp_table=result_table.iloc[:,i:i+2]
              stay_list.append(set(temp_table[temp_table.iloc[:,0]==1]['columns'].values))
          for l in  stay_list:
              if stay_set:
                 stay_set=stay_set & l
              else:
                 stay_set=l
          ret_data=self.data[list(stay_set)]                 
          if self.ignore_columns:      
             ret_data=self.recover_func(ret_data)
          return ret_data,list(stay_set)
