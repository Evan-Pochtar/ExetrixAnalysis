import sys, os, time, tracemalloc, json, threading
from collections import defaultdict, deque
import traceback
from jinja2 import Template

try:
    import resource
except ImportError:
    resource = None

try:
    import psutil
except ImportError:
    psutil = None

def usage():
    print("Usage: profiler_wrapper.py --report-dir <dir> <target_script> [args...]")
    sys.exit(2)

def is_user_code(frame):
    filename = frame.f_code.co_filename
    
    if filename.startswith('<'):
        return False
    
    if any(path in filename for path in [
        'site-packages', 'lib/python', 'lib64/python',
        '/usr/lib/python', '/usr/local/lib/python',
        'importlib', 'pkgutil', 'zipimport'
    ]):
        return False
    
    # skip framework noise
    function_name = frame.f_code.co_name
    if function_name in ['<module>', '__init__', '__enter__', '__exit__']:
        return False
    
    return True

def make_function_id_from_frame(frame):
    co = frame.f_code
    mod = frame.f_globals.get("__name__", "<module>")
    filename = os.path.basename(co.co_filename)
    name = co.co_name
    return f"{mod}.{name}() [{filename}]"

def make_function_id_from_cfunc(cfunc):
    # only track C functions that are likely to be user-relevant
    name = getattr(cfunc, "__name__", str(cfunc))
    if name in ['print', 'len', 'range', 'enumerate', 'zip', 'map', 'filter']:
        return None
    return f"<builtin>.{name}()"

def profiler_main(report_dir, target_argv):
    stats = {}  # id -> {'total_time': float, 'call_count': int, 'children_time': float}
    edges = defaultdict(lambda: {'call_count':0, 'total_time':0.0})
    lock = threading.Lock()
    call_stack = deque()  # stack of (func_id, start_time, is_user_code)

    # list of (t, current_bytes, peak_bytes)
    mem_samples = []
    sampling = True

    def mem_sampler():
        while sampling:
            cur, peak = tracemalloc.get_traced_memory()
            mem_samples.append((time.perf_counter(), cur, peak))
            time.sleep(0.05)

    tracemalloc.start(25)

    def prof(frame, event, arg):
        nonlocal call_stack, stats, edges
        t = time.perf_counter()
        
        if event == 'call':
            is_user = is_user_code(frame)
            if is_user:
                fid = make_function_id_from_frame(frame)
                call_stack.append((fid, t, True))
            else:
                call_stack.append((None, t, False))
                
        elif event == 'return':
            if not call_stack:
                return
            fid, start, is_user = call_stack.pop()
            if not is_user:
                return
                
            dur = t - start
            with lock:
                st = stats.setdefault(fid, {'total_time':0.0, 'call_count':0, 'children_time':0.0})
                st['total_time'] += dur
                st['call_count'] += 1
                
                parent_id = None
                for parent_fid, parent_start, parent_is_user in reversed(call_stack):
                    if parent_is_user:
                        parent_id = parent_fid
                        break
                
                if parent_id:
                    pst = stats.setdefault(parent_id, {'total_time':0.0, 'call_count':0, 'children_time':0.0})
                    pst['children_time'] += dur
                    ekey = (parent_id, fid)
                    edges[ekey]['call_count'] += 1
                    edges[ekey]['total_time'] += dur
                    
        elif event == 'c_call':
            fid = make_function_id_from_cfunc(arg)
            if fid:
                call_stack.append((fid, t, True))
            else:
                call_stack.append((None, t, False))
                
        elif event == 'c_return':
            if not call_stack:
                return
            fid, start, is_user = call_stack.pop()
            if not is_user:
                return
                
            dur = t - start
            with lock:
                st = stats.setdefault(fid, {'total_time':0.0, 'call_count':0, 'children_time':0.0})
                st['total_time'] += dur
                st['call_count'] += 1
                
                parent_id = None
                for parent_fid, parent_start, parent_is_user in reversed(call_stack):
                    if parent_is_user:
                        parent_id = parent_fid
                        break
                
                if parent_id:
                    pst = stats.setdefault(parent_id, {'total_time':0.0, 'call_count':0, 'children_time':0.0})
                    pst['children_time'] += dur
                    ekey = (parent_id, fid)
                    edges[ekey]['call_count'] += 1
                    edges[ekey]['total_time'] += dur

    sampler = threading.Thread(target=mem_sampler, daemon=True)
    sampler.start()

    sys.setprofile(prof)
    try:
        threading.setprofile(prof)
    except Exception:
        pass

    target_path = target_argv[0]
    target_args = target_argv[1:]
    sys.argv = [target_path] + target_args
    globals_dict = {"__name__": "__main__", "__file__": target_path, "__package__": None}

    start_wall = time.perf_counter()
    start_cpu = time.process_time()
    exit_code = 0
    script_dir = os.path.dirname(os.path.abspath(target_path))

    os.chdir(script_dir)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    try:
        with open(target_path, 'rb') as f:
            code = compile(f.read(), target_path, 'exec')
            exec(code, globals_dict, globals_dict)
    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 0
    except Exception as e:
        traceback.print_exc()
        exit_code = 1
    finally:
        end_wall = time.perf_counter()
        end_cpu = time.process_time()
        sys.setprofile(None)
        sampling = False
        sampler.join(timeout=1.0)
        tracemalloc.stop()

    nodes = []
    for fid, v in stats.items():
        total = v.get('total_time', 0.0)
        children = v.get('children_time', 0.0)
        exclusive = total - children
        nodes.append({
            'id': fid,
            'total_time': total,
            'call_count': v.get('call_count', 0),
            'children_time': children,
            'exclusive_time': exclusive
        })

    nodes.sort(key=lambda x: x['total_time'], reverse=True)

    edge_list = []
    for (p,c), info in edges.items():
        edge_list.append({
            'caller': p,
            'callee': c,
            'call_count': info['call_count'],
            'total_time': info['total_time']
        })
    
    edge_list.sort(key=lambda x: x['total_time'], reverse=True)

    peak_rss = None
    if resource is not None:
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            peak_rss = getattr(usage, 'ru_maxrss', None)
        except Exception:
            pass
    elif psutil is not None:
        try:
            process = psutil.Process(os.getpid())
            mem = process.memory_info()
            peak_rss = getattr(mem, "peak_wset", None) or getattr(mem, "rss", None)
        except Exception:
            pass

    report = {
        'meta': {
            'language': 'python',
            'command': [target_path] + target_args,
            'wall_time_s': end_wall - start_wall,
            'cpu_time_s': end_cpu - start_cpu,
            'exit_code': exit_code,
            'timestamp': time.time(),
        },
        'nodes': nodes,
        'edges': edge_list,
        'memory_samples': [
            {'t': t - mem_samples[0][0] if mem_samples else 0, 'current': cur, 'peak': peak} 
            for (t, cur, peak) in mem_samples
        ],
        'peak_rss': peak_rss
    }

    os.makedirs(report_dir, exist_ok=True)
    
    report_path = os.path.join(report_dir, "report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    html_report_path = os.path.join(report_dir, "report.html")
    generate_html_report(report, html_report_path, report_dir)

    print(f"Profiler finished. Reports: {report_path}, {html_report_path}")

def generate_html_report(report_data: dict, output_path: str, report_dir: str) -> None:
    template_path = os.path.join(report_dir, "../src/html/reportTemplate.html")
    
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_code = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(
            f"HTML template file not found at {template_path}. "
        )
    
    meta = report_data.get('meta', {})
    nodes = report_data.get('nodes', [])
    edges = report_data.get('edges', [])
    memory_samples = report_data.get('memory_samples', [])
    
    total_functions = len(nodes)
    max_time = max((n['total_time'] for n in nodes), default=0)
    top_functions = nodes[:10]
    
    template = Template(template_code)
    html_output = template.render(
        meta=meta,
        nodes=nodes,
        edges=edges,
        memory_samples=memory_samples,
        total_functions=total_functions,
        max_time=max_time,
        top_functions=top_functions,
        language=meta.get('language', 'unknown'),
        wall_time=meta.get('wall_time_s', 0),
        cpu_time=meta.get('cpu_time_s', 0),
        peak_rss=report_data.get('peak_rss'),
        report_json=json.dumps(report_data)
    )
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_output)

def parse_args(argv):
    if '--report-dir' not in argv:
        usage()

    i = argv.index('--report-dir')

    if i+1 >= len(argv):
        usage()

    report_dir = argv[i+1]

    if '--' not in argv:
        usage()

    j = argv.index('--')
    target = argv[j+1:]

    if not target:
        usage()

    return report_dir, target

if __name__ == "__main__":
    report_dir, target = parse_args(sys.argv[1:])
    profiler_main(report_dir, target)
