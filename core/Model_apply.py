# -*- coding: utf-8 -*-


"""
"""

import pandas as pd 
import numpy as np 
from sklearn.metrics import roc_auc_score
from Bin_Transform import Bin
from Weight_of_evidence import Woe_dataframe
from sklearn.externals import joblib
from Common_tools import Basesteps


__version__ = '0.0.2'


class Evaluate(Basesteps):

      def __init__(self,data,
                        target,
                        fitted_model,
                        ignore_columns=None):
          super(Evaluate,self).__init__(data,target,ignore_columns)
          self.fitted_model=fitted_model

      @property
      def auc(self):
          print 'AUC Value:'
          return roc_auc_score(self.target, self.fitted_model.predict_proba(self.data)[:,1])


      @property
      def ks(self):
          print 'KS Value:'
          return Apply_func.ksvalue(self.data.values,self.target.values,self.fitted_model)

      @property
      def from_p_to_score(self):
          score_out,detail_out=Apply_func.fromptoscore(self.fitted_model,self.data.values)
          score=pd.DataFrame(score_out,columns=['score'],index=self.data.index)
          detail=pd.DataFrame(detail_out,index=self.data.index).drop(0,1)
          detail.columns=self.data.columns
          final_df=pd.concat(([score,detail]),axis=1)
          if self.ignore_columns:
             final_df=self.recover_func(final_df)
          return final_df


class Test_apply(Basesteps):

      def __init__(self,data,
                        target,
                        bin_dictionary,
                        woe_dict,
                        final_columns,
                        ignore_columns=None):
          super(Test_apply,self).__init__(data,target,ignore_columns)
          self.bin_dictionary=bin_dictionary
          self.woe_dict=woe_dict
          self.final_columns=final_columns




      def __call__(self):
          bin_ins=Bin(self.data,target=None)
          dis_df=bin_ins.transform(self.bin_dictionary)
          woe_ins=Woe_dataframe(dis_df,target=None)
          woe_test=woe_ins.woe_transform(self.woe_dict)
          woe_test=woe_test[self.final_columns]
          if self.ignore_columns:
             woe_test=self.recover_func(woe_test)
          return woe_test


class Apply_func(object):

      @staticmethod
      def ksvalue(data,target,model):
          prob=model.predict_proba(data)
          y=pd.DataFrame(target,columns=['flag'])  
          list1=np.digitize(prob[:,1],np.array([np.percentile(prob[:,1], x) for x in range(10, 101, 10)]),right=True)+1
          probpd=pd.DataFrame(np.array(list1),columns=['decimal'])
          resultpd=pd.merge(probpd,y,how='left',left_index=True,right_index=True)
          resultpd_st=resultpd.groupby('decimal',group_keys=False)['flag'].apply(pd.value_counts).unstack()
          resultpd_st2=resultpd_st.apply(lambda x:x*1.0/x.sum(),axis=0).apply(np.cumsum,axis=0)
          resultpd_st2.columns=['normal','dlq']
          resultpd_st2.plot()
          return abs(resultpd_st2['dlq']-resultpd_st2['normal']).max() 

      @staticmethod
      def fromptoscore(model,data,A=540.6843, B=86.5617):
          constant=np.array([1]*data.shape[0]).reshape(-1,1)
          X_new=np.column_stack((constant,data)).astype('float64')
          coef_new=np.column_stack((A-B*(model.intercept_),-B*(model.coef_))).astype('float64').T
          detail=np.zeros(X_new.shape)
          for i in range(len(X_new)):
                      for k in range(len(coef_new)):
                          detail[i][k]=X_new[i][k]*coef_new[k]
          return np.dot(X_new,coef_new),detail

      @staticmethod
      def save(bin_dictionary,woe_dict,final_columns,model,test_score,path):
          model_save={}
          model_save['model']=model
          model_save['bin_dictionary']=bin_dictionary
          model_save['woe_dict']=bin_dictionary
          model_save['final_columns']=bin_dictionary
          model_save['test_score']=test_score                  #store test score for psi test
          try: 
              joblib.dump(model_save,path,compress=True)
          except Exception,e:
              print str(e)

      @staticmethod
      def load(path):
          return joblib.load(path)

 

class PSI(object):
      def __init__(self,test,real):
          self.test=test
          self.real=real
          self.main()


      def main(self):
          bin_=range(10, 101, 10)
          bins=np.array(np.percentile(self.test, bin_))
          bins=np.sort(list(set(np.round(bins.tolist(),3))))
          test_bin=pd.Series(np.digitize(self.test, bins,right=True))
          real_bin=pd.Series(np.digitize(self.real, bins,right=True))
          test_ratio=test_bin.value_counts().sort_index()/test_bin.sum()
          real_ratio=real_bin.value_counts().sort_index()/real_bin.sum()
          final_df=pd.concat([test_ratio,real_ratio],axis=1,join='inner')
          final_df.columns=['test','real']
          final_df['minus']=final_df['real']-final_df['test']
          final_df['In']=(final_df['real']/final_df['test']).apply(np.log)
          final_df['psi']=final_df['minus']*final_df['In']
          self.final_df=final_df
          self.bins=bins

      @property
      def final_df(self):
          return self.final_df

      @property
      def bins(self):
          return self.bins

      @property
      def psi(self):
          return self.final_df.psi.sum()     