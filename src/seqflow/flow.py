#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import functools
import sys
import shutil
from pathos.multiprocessing import ProcessingPool as Pool
import anytree
from anytree.exporter import DotExporter
from loguru import logger

logger.remove()
logger.add(sys.stderr, format="<light-green>[{time:YYYY-MM-DD HH:mm:ss}]</light-green> <level>{message}</level>",
           filter=lambda record: record["level"].name == "TRACE",
           level="TRACE")
logger.add(sys.stderr, format="<level>{message}</level>", filter=lambda record: record["level"].name == "DEBUG")
logger.add(sys.stderr, format="<light-green>[{time:HH:mm:ss}]</light-green> <level>{message}</level>", level="INFO")

class task:
    tasks = {}
    
    def __init__(self, inputs=None, outputs=None, kwargs=None, kind='', parent=None, follow=None, processes=1,
                 mkdir_before_run=None, cleanup_after_run=None, force_cleanup_on_error=False):
        self.inputs = inputs
        self.outputs = outputs
        self.kwargs = kwargs
        self.kind = kind
        self.parent = parent
        self.follow = follow
        self.processes = processes
        self.dirs = mkdir_before_run
        self.cleanups = cleanup_after_run
        self.force_cleanup = force_cleanup_on_error

    def __call__(self, function):
        self.function = function
        self.description = function.__doc__
        task.tasks[function.__name__] = Task(function.__name__, function.__doc__, self.inputs,
                                             self.outputs, self.kind, self.parent, self.follow,
                                             self.processes, self.dirs, self.cleanups, self.force_cleanup,
                                             self.function)
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            result = function(*args, **kwargs)
            return result
        return wrapper


def touch_file(filename):
    with open(filename, 'w') as o:
        o.write('')


def make_folder(folder):
    if not os.path.isdir(folder):
        try:
            os.mkdir(folder)
        except OSError:
            raise OSError(f'Cannot create directory {folder}!')


def delete_path(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            try:
                os.unlink(path)
            except OSError:
                logger.warning('Failed to delete file {path}.')
        try:
            shutil.rmtree(path)
        except Exception as e:
            logger.warning('Failed to delete directory {path}.')


class Task(anytree.NodeMixin):
    def __init__(self, name, description='', inputs='', outputs='', kind='', parent=None,
                 follow=None, processes=1, dirs=None, cleanups=None, force_cleanup=False, executor=None,
                 children=None):
        super(Task, self).__init__()
        self.name = name
        self.description = description if description else ''
        self.short_description = description.strip().splitlines()[0] if description else ''
        self.kind = kind
        self.inputs = inputs
        self.outputs = outputs
        self.yield_outputs = None
        self.parent = None
        if parent is None:
            parent_name = inputs.__name__ if callable(inputs) else None
        else:
            if callable(parent):
                if callable(inputs):
                    if inputs.__name__ != parent.__name__:
                        raise ValueError(f'In task {name}: specified parent {parent.__name__} does not match '
                                         f'inferred parent {inputs.__name__}.')
                parent_name = parent.__name__
            else:
                raise TypeError(f'In task {name}, invalid parent type was encountered.')
        self.parent_name = parent_name
        self.follow = follow
        self.processes = processes
        self.dirs = dirs if dirs else []
        self.cleanups = cleanups if cleanups else []
        self.force_cleanup = force_cleanup
        self.executor = executor
        self.processor = None
    
    def process(self, dry=True, processes=1, verbose=False):
        if self.outputs:
            if isinstance(self.inputs, (str, list)):
                pass
            elif callable(self.outputs):
                if not self.inputs:
                    raise TypeError(f'In task {self.name}, invalid type has been specified for inputs when outputs '
                                    f'were specified using a callable object.')
            else:
                raise TypeError(f'In task {self.name}, invalid type has been specified for outputs.')
        if self.inputs:
            if isinstance(self.inputs, str):
                if callable(self.outputs):
                    self.outputs = self.outputs(self.inputs)
            elif isinstance(self.inputs, list):
                if callable(self.outputs):
                    self.outputs = [self.outputs(i) for i in self.inputs]
            else:
                raise TypeError(f'In task {self.name}, invalid type has been specified for inputs.')
        
        kind, inputs, outputs = '', [], []
        if self.inputs:
            if self.outputs:
                if isinstance(self.inputs, str):
                    kind = 'transform' if isinstance(self.outputs, str) else 'split'
                    inputs = [self.inputs]
                    outputs = [self.outputs]
                elif isinstance(self.inputs, list):
                    if isinstance(self.outputs, str):
                        kind = 'merge'
                        inputs = [self.inputs]
                        outputs = [self.outputs]
                    else:
                        kind = 'transform'
                        inputs = self.inputs
                        outputs = self.outputs
            else:
                kind = 'delete'
                if isinstance(self.inputs, str):
                    inputs = [self.inputs]
                    outputs = [None]
                else:
                    inputs = self.inputs
                    outputs = [None] * len(self.inputs)
        else:
            if self.outputs:
                kind = 'create'
                if isinstance(self.outputs, str):
                    inputs = [None]
                    outputs = [self.outputs]
                else:
                    inputs = [None] * len(self.outputs)
                    outputs = self.outputs
            else:
                raise ValueError(f'In task {self.name}, neither inputs nor outputs has been specified.')
        
        if self.kind:
            msg = f'In task {self.name}, task kind {self.kind} does not match inferred task kind {kind}!'
            assert self.kind == kind, ValueError(msg)
        li, lo = len(inputs), len(outputs)
        assert li == lo, (f'In task {self.name}, the number of items in inputs ({li}) does not match '
                          f'the number of items in outputs ({lo})!')

        need_to_update, inputs_need_to_update, outputs_need_to_update = [], [], []
        need_to_create = [d for d in self.dirs if not os.path.exists(d)]
        need_to_cleanup = [c for c in self.cleanups if os.path.exists(c)]
        for i, o in zip(inputs, outputs):
            if i is None:
                i1 = 'None'
            elif isinstance(i, str):
                i1 = i
            elif isinstance(i, (list, tuple)):
                i1 = i[0]
            else:
                raise TypeError(f'Invalid type for inputs item: {i}.')
            if o is None:
                o1 = 'None'
            elif isinstance(o, str):
                o1 = o
            elif isinstance(o, (list, tuple)):
                o1 = o[0]
            else:
                raise TypeError(f'Invalid type for outputs item: {o}.')
            if kind in ('transform', 'split', 'merge'):
                if os.path.exists(o1) and os.path.getmtime(o1) >= os.path.getmtime(i1):
                    continue
            elif kind == 'create':
                if os.path.exists(o1):
                    continue
            elif kind == 'delete':
                if not os.path.exists(i):
                    continue
            need_to_update.append([i1, o1])
            inputs_need_to_update.append(i)
            outputs_need_to_update.append(o)
        if need_to_update:
            if len(need_to_update) == 1 or self.processes == 1 or processes == 1:
                process_mode, processes = 'sequential mode', 1
            else:
                processes = min([processes, self.processes])
                process_mode = f'parallel mode ({processes} processes)'
            run_mode = 'sequential' if len(need_to_update) == 1 or self.processes == 1 else 'parallel'
            if dry:
                create_list = '\n    '.join(need_to_create)
                if create_list:
                    create_list = f'The following director(ies) will be created:\n    {need_to_create}\n'
                update_list = '\n    '.join([f'{i} --> {o}' for i, o in need_to_update])
                if update_list:
                    update_list = (f'The following file(s) will be {kind}{"d" if kind.endswith("e") else "ed"} '
                                   f'in {process_mode}:\n    {update_list}\n')
                cleanup_list = '\n    '.join(need_to_cleanup)
                if cleanup_list:
                    cleanup_list = f'The following file(s) will be deleted:\n    {cleanup_list}\n'
                msg = '\n'.join([s for s in (create_list, update_list, create_list) if s])
                logger.debug(f'Task [{self.name}]:\n{msg}')
            else:
                if need_to_create:
                    _ = [make_folder(d) for d in need_to_create]
                if verbose:
                    logger.debug(f'Process task {self.name} in {process_mode}.')
                if 'sequential' in process_mode:
                    outputs = [self.executor(i, o) for i, o in need_to_update]
                else:
                    with Pool(processes=processes) as pool:
                        outputs = pool.map(self.executor, inputs_need_to_update, outputs_need_to_update)
                self.yield_outputs = outputs
                if need_to_cleanup:
                    _ = [delete_path(c) for c in need_to_cleanup]
        else:
            logger.debug(f'Task {self.name} already up to date.')
    
        
class Flow:
    def __init__(self, name, description='', short_description=''):
        self.name = name
        if not isinstance(name, str):
            raise TypeError('Workflow name must be as string!')
        self.description = description
        if not isinstance(description, str):
            raise TypeError('Workflow description must be as string!')
        self.short_description = short_description or description.splitlines()[0]
        if not isinstance(self.short_description, str):
            raise TypeError('Workflow short_description must be as string!')
        
        flow = anytree.Node(self.name, description=self.description, short_description=self.short_description)
        tasks = task().tasks
        ancestry = [v for k, v in tasks.items() if v.parent_name is None]
        if len(ancestry) == 1:
            ancestry = ancestry[0]
            ancestry.parent = flow
            nodes = {ancestry.name: ancestry}
            tasks.pop(ancestry.name, ancestry)
        else:
            orphans = [v.name for v in ancestry]
            orphans = '\n  '.join(orphans)
            raise ValueError(f'Two many orphan tasks, start point of {name} cannot be determined.\n'
                             f'Check the following tasks:\n  {orphans}')

        for name, work in tasks.items():
            parent = nodes[work.parent_name]
            work.parent = parent
            inputs = work.inputs
            if callable(inputs):
                inputs = parent.outputs
                if callable(inputs):
                    inputs = [inputs(i) for i in parent.inputs]
            work.inputs = inputs
            nodes[name] = work
        self.flow = flow
        
    def list_tasks(self):
        tasks = [f'{i:02d}. {node.name}' for i, (_, _, node) in enumerate(anytree.RenderTree(self.flow), 0)
                 if not node.is_root]
        task_list = "\n  ".join(tasks)
        logger.debug(f'{self.name} consists of the following {len(tasks)} task(s):\n  {task_list}')
        
    def run(self, dry=False, processes=1, verbose=False):
        for pre, _, node in anytree.RenderTree(self.flow):
            if not node.is_root:
                node.process(dry=dry, processes=processes, verbose=verbose)
            
    def print_out(self, style='continued'):
        styles = {'ascii': anytree.render.AsciiStyle(),
                  'continued': anytree.render.ContStyle(),
                  'continue_rounded': anytree.render.ContRoundStyle(),
                  'double': anytree.render.DoubleStyle()}
        if style not in styles:
            logger.error(f'Invalid style: {style}, using continue_rounded style instead.\nValid style can be one of '
                         f'these: {", ".join(styles)}.')
            style = 'continue'
        for pre, _, node in anytree.RenderTree(self.flow, style=styles[style]):
            logger.debug(f'{pre}[{node.name}] {node.short_description}')
    
    def flow_chart(self, chart=''):
        if not chart:
            raise ValueError('No chart output was specified!')
        DotExporter(self.flow, graph="graph", nodeattrfunc=lambda node: "shape=box",
                    edgetypefunc=lambda node, child: '--').to_picture(chart)
    
        
if __name__ == '__main__':
    pass
