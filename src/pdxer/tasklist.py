import pandas as pd
import datetime

from xerparser.reader import Reader
from rich import print
from time import perf_counter

class TaskListHandler:

    def __init__(self, df):
        self.df = df
        
    def filter(self, min_start: str = None, max_start: str = None, min_end : str = None, max_end : str = None, task_types_or: list=None, task_name: str=None):
        """
        Filter dataframe rows based on minimal start date, maximal end date, list of task types and task names
        """
        
        df = self.df
        masks = []
        if not min_start is None:
            print(f"filtering on start date > {min_start}")
            masks.append(df['start_date'] > min_start)

        if not max_start is None:
            print(f"filtering on start date < {max_start}")
            masks.append(df['start_date'] < max_start)

        if not min_end is None:
            print(f"filtering on end date > {min_end}")
            masks.append(df['end_date'] < min_end)

        if not max_end is None:
            print(f"filtering on end date < {max_end}")
            masks.append(df['end_date'] < max_end)
            
        if not task_types_or is None:
            type_masks = [(df['task_type']==t) for t in task_types_or]
            type_mask = type_masks[0]
            for m in type_masks[1:]:
                type_mask |= m
                
            masks.append(type_mask)
    
        if not task_name is None:
            masks.append(df['task_name'].str.contains(task_name, case=False))
    
        if not masks:
            return df
    
        mask = masks[0]
        for m in masks[1:]:
            mask &= m
    
        return df[mask]

    def get_selection(self, task_codes):
        """
        Return a subset by row
        """
        return self.df[self.df.index.isin(task_codes)]

    def get_successors(self, task_code: str, levels: int=0):
        """
        Select successor tasks of 'task_code'
        """
        df = self.df
        
        s = df.loc[task_code].successors
        return df[df['task_id'].isin(s)]


    def get_predecessors(self, task_code: str, levels: int=0):
        """
        Select predecessor tasks of 'task_code'
        """
        df = self.df
        
        s = df.loc[task_code].predecessors
        return df[df['task_id'].isin(s)]
        
def list_projects(filename):
    xer = Reader(filename)

    print(f"Projects in '{filename}'")
    for p in xer.projects:
        print(f"'{p}': {len(p.activities)}")

