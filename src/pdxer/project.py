import pandas as pd
import datetime

from xerparser.reader import Reader
from rich import print
from time import perf_counter

from .tasklist import TaskListHandler

class ProjHandler(TaskListHandler):
    """ProjHandler

    Args:
        TaskListHandler (_type_): _description_
    """

    def __init__(self, filename: str, projname: str):
        """Create a ProjectHandler class from a project in a  P6 Xer file 

        Args:
            filename (str): Name of the P6 XER file
            projname (str): Name of the project
        """
        self.filename = filename
        self.projname = projname
        print(f"Loading '{projname}' from '{filename}'")
        t_start = perf_counter() 
        self.proj = self._load_proj()
        t_middle = perf_counter() 
        print(f"Loaded '{projname}' [{t_middle-t_start:.2f}s]")
        self.task_id_to_name_map = { a.task_id:a.task_name for a in self.proj.activities}
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
        self.common_columns=common
        pass


    def merge(self, proj_a, proj_b, how='left'):
        suff = ('', '_other')
        x = pd.merge(proj_a.df, proj_b.df, how=how, on=self.common_columns, suffixes=suff)
        x['start_diff'] = x[f'start_date{suff[0]}']-x[f'start_date{suff[1]}']
        x['end_diff'] = x[f'end_date{suff[0]}']-x[f'end_date{suff[1]}']
        x = x.sort_values(f'end_date{suff[0]}')
        # print(f"a: {len(proj_a.df)} b: {len(proj_b.df)} merge: {len(x)}")
        return x