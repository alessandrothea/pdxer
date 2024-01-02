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
        return self.df[self.df.index.isin(task_codes)]


    def get_successors(self, task_code: str):
        """
        Select successor tasks of 'task_code'
        """
        df = self.df
        
        s = df.loc[task_code].successors
        return df[df['task_id'].isin(s)]


    def get_predecessors(self, task_code: str):
        """
        Select predecessor tasks of 'task_code'
        """
        df = self.df
        
        s = df.loc[task_code].predecessors
        return df[df['task_id'].isin(s)]
        

class ProjHandler(TaskListHandler):

    def __init__(self, filename, projname):
        self.filename = filename
        self.projname = projname
        print(f"Loading '{projname}' from '{filename}'")
        t_start = perf_counter() 
        self.proj = self._load_proj()
        t_middle = perf_counter() 
        print(f"Loaded '{projname}' [{t_middle-t_start:.2f}s]")
        print(f"Converting '{projname}' into Pandas Dataframe")
        df = self._activities_to_df()
        t_end = perf_counter() 
        print(f"'{projname}' Dataframe ready [{t_end-t_start:.2f}s]")
        super().__init__(df)


    def _load_proj(self):
        xer = Reader(self.filename)
        projs = { str(p):p for p in xer.projects}
        if not self.projname in projs:
            return None
        return projs[self.projname]

    def _find_activity_by_task_code(self, task_code):
        return next(iter(t for t in self.proj.activities if t.task_code == task_code), None)

    def _find_activity_by_task_id(self, task_id):
        return next(iter(t for t in self.proj.activities if t.task_id == task_id), None)
    
    def _activities_to_df(self, add_prec_succ: bool = True):
        fields = ["task_id", "task_code", "task_name", "task_type", "start_date", "end_date"]
        
        # Extract column types from first activity
        a = self.proj.activities[0]
        col_types = {}
        
        for f in fields:
            v = getattr(a, f)
            t = type(v)
            dt = t if not t is datetime.datetime else 'datetime64[s]'
            col_types[f] = dt
    
        if add_prec_succ:
            col_types['predecessors'] = 'object'
            col_types['successors'] = 'object'
        
        # Arrange field values in a dictionary of lists
        values = {}
        # for a in daq_acts:
        for a in self.proj.activities:
            for f in fields:
                values.setdefault(f,[]).append(getattr(a,f))
    
            if add_prec_succ:
                # VERY SLOW
                # predecessor_tasks = [next(iter(t for t in proj.activities if t.task_id == p.pred_task_id), None) for p in a.predecessors]
                # values.setdefault('predecessors',[]).append([ t.task_code for t in predecessor_tasks if t is not None])
                # successor_tasks = [next(iter(t for t in proj.activities if t.task_id == s.task_id), None) for s in a.successors]
                # values.setdefault('successors',[]).append([t.task_code for t in successor_tasks if t is not None])
                values.setdefault('predecessors',[]).append([t.pred_task_id for t in a.predecessors])
                values.setdefault('successors',[]).append([t.task_id for t in a.successors])
        
        # And then convert the lists to pd series
        series = {
            f:pd.Series(data=values[f], dtype=col_types[f])
            for f in col_types
        }
        
        # Finally, build the dataframe
        df = pd.DataFrame(series)
        df.set_index('task_code', inplace=True)
        df.sort_values('end_date', inplace=True)

        return df


class ProjComparator:
    def __init__(self, common=['task_code', 'task_name', 'task_type']):
        pass

    def merge(self, proj_a, proj_b):
        suff = ('', '_other')
        x = pd.merge(proj_a.df, proj_b.df, on=['task_code', 'task_name', 'task_type'],suffixes=suff)
        x['start_diff'] = x[f'start_date{suff[0]}']-x[f'start_date{suff[1]}']
        x['end_diff'] = x[f'end_date{suff[0]}']-x[f'end_date{suff[1]}']
        x = x.sort_values(f'end_date{suff[0]}')
        return x
    

def list_projects(filename):
    xer = Reader(filename)

    print(f"Projects in '{filename}'")
    for p in xer.projects:
        print(f"{p}: {len(p.activities)}")