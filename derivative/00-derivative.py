import ipywidgets as widgets
from ipywidgets import Layout, Button, Box, FloatText, Textarea, Dropdown, Label, IntSlider,Button, HBox, VBox
import os
import yaml
import derivative_apply
reload(derivative_apply)
from derivative_apply import apply_func
from derivative import load_config

describe = widgets.Label(value='Please Type the config text')
style = {'description_width': 'initial'}

config_text=widgets.Textarea(
    placeholder='Type your config',
    #description='Please enter the config text',
    layout=Layout(width='60%', height='400px'),
    disabled=False,
    style=style
)

form_item_layout = Layout(
    display='flex',
    flex_flow='row',
    justify_content='space-between'
)


period_orderby=widgets.ToggleButtons(
    options=['user', 'specific_date'],
    disabled=False,
    button_style='', # 'success', 'info', 'warning', 'danger' or ''
)



period_ascending=widgets.ToggleButtons(
    options=['True', 'False'],
    value='True',
    disabled=False,
    button_style='', # 'success', 'info', 'warning', 'danger' or ''
)


period_date=widgets.DatePicker(
    disabled=False
)

apply_type=widgets.Dropdown(
    options={'loose': 'loose', 'strict': 'strict'},
    value='loose',
)


output_table=widgets.Text(placeholder='table store in scorecard schema')



form_items = [
    Box([Label(value='period_orderby '), period_orderby], layout=form_item_layout),
    Box([Label(value='period_ascending'),
         period_ascending], layout=form_item_layout),
    Box([Label(value='specific_date'),
         period_date], layout=form_item_layout),
    Box([Label(value='apply_type'),
         apply_type], layout=form_item_layout),
    Box([Label(value='output_table'),
         output_table], layout=form_item_layout)

]

form = Box(form_items, layout=Layout(
    display='flex',
    flex_flow='column',
    #border='solid 2px',
    align_items='stretch',
    width='50%'
))

save=Button(description='Save',button_style='warning')
load=Button(description='Load',button_style='warning')
save_text=widgets.Text(description='save_path')
load_text=widgets.Text(description='load_path')

save_load=HBox([HBox([save,load]),VBox([save_text,load_text])])


Progress=widgets.IntProgress(
    min=0,
    value=0,
    step=1,
    description='Progress:',
    bar_style='success',
    orientation='horizontal'
)
#    
#    max=10.0,
#    

run=Button(description='Run',button_style='info')

run_show=Box((run,Progress),layout=Layout(justify_content='flex-start'))


def  function_save(x):
     save_path=save_text.value
     config_detail=config_text.value
     if len(save_path) and len(config_detail):
        with open(save_path,'w') as f:
             f.write(config_detail)   
        print 'Success save the config'
     else:
        print "can't save the config"
        
        
def  function_load(x):
     load_path=load_text.value
     if len(load_path):
        with open(load_path,'r') as f:
            con_temp=yaml.load(f)
            ck=load_config(load_path)
            ck.check(con_temp)
            _config=yaml.dump(con_temp)
        config_text.value=_config
        print 'Success load the config'
     else:
        print "can't load the config"
        

def  function_run(x):
     orderby_value=period_orderby.value
     ascending_value={'True':True,'False':False}[period_ascending.value]
     if period_date.value:
        date_value=period_date.value.strftime('%Y-%m-%d')
     else:
        date_value=period_date.value
     apply_type_value=apply_type.value
     output_value=output_table.value
     config=yaml.load(config_text.value)
     if len(config) and len(output_value):
         _run=apply_func(config,output_value,orderby_value,ascending_value,date_value,Progress,apply_type_value)  
         _run.main_apply()   
     else:
        print "can't start job"    
    
        
        
        
    
save.on_click(function_save)
load.on_click(function_load)
run.on_click(function_run)


def start():
    display(describe,save_load,config_text,form,run_show)
    print '''

    Logging info:
    ''' 

