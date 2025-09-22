import sys, os, time, tracemalloc, json, threading
from collections import defaultdict, deque
import traceback
import gc

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
    name = getattr(cfunc, "__name__", str(cfunc))
    if name in ['print', 'len', 'range', 'enumerate', 'zip', 'map', 'filter']:
        return None
    return f"<builtin>.{name}()"

def get_system_info():
    info = {'python_version': sys.version.split()[0]}
    if psutil:
        try:
            info.update({
                'cpu_count': psutil.cpu_count(),
                'cpu_freq': psutil.cpu_freq()._asdict() if psutil.cpu_freq() else None,
                'total_memory': psutil.virtual_memory().total,
                'available_memory': psutil.virtual_memory().available,
            })
        except Exception:
            pass
    return info

def profiler_main(report_dir, target_argv):
    stats = {}
    edges = defaultdict(lambda: {'call_count':0, 'total_time':0.0})
    lock = threading.Lock()
    call_stack = deque()

    mem_samples = []
    cpu_samples = []
    gc_stats = []
    sampling = True
    start_time = time.perf_counter()

    def system_sampler():
        sample_count = 0
        while sampling:
            t = time.perf_counter() - start_time
            try:
                cur_mem, peak_mem = tracemalloc.get_traced_memory()
            except Exception:
                cur_mem = 0
                peak_mem = 0
            mem_samples.append({
                't': t,
                'current': cur_mem,
                'peak': peak_mem
            })
            if psutil:
                try:
                    process = psutil.Process()
                    cpu_percent = process.cpu_percent()
                    memory_info = process.memory_info()
                    cpu_samples.append({
                        't': t,
                        'cpu_percent': cpu_percent,
                        'rss': memory_info.rss,
                        'vms': memory_info.vms
                    })
                except Exception:
                    pass
            if sample_count % 20 == 0:
                try:
                    gc_gen_stats = gc.get_stats()
                except Exception:
                    gc_gen_stats = []
                gc_counts = gc.get_count()
                gc_stats.append({
                    't': t,
                    'counts': gc_counts,
                    'collections': [gen.get('collections', 0) for gen in gc_gen_stats] if gc_gen_stats else []
                })
            sample_count += 1
            time.sleep(0.05)

    tracemalloc.start(25)
    gc.set_debug(0)

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
                st = stats.setdefault(fid, {
                    'total_time': 0.0,
                    'call_count': 0,
                    'children_time': 0.0,
                    'min_time': float('inf'),
                    'max_time': 0.0
                })
                st['total_time'] += dur
                st['call_count'] += 1
                st['min_time'] = min(st['min_time'], dur)
                st['max_time'] = max(st['max_time'], dur)
                parent_id = None
                for parent_fid, parent_start, parent_is_user in reversed(call_stack):
                    if parent_is_user:
                        parent_id = parent_fid
                        break
                if parent_id:
                    pst = stats.setdefault(parent_id, {
                        'total_time': 0.0,
                        'call_count': 0,
                        'children_time': 0.0,
                        'min_time': float('inf'),
                        'max_time': 0.0
                    })
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
                st = stats.setdefault(fid, {
                    'total_time': 0.0,
                    'call_count': 0,
                    'children_time': 0.0,
                    'min_time': float('inf'),
                    'max_time': 0.0
                })
                st['total_time'] += dur
                st['call_count'] += 1
                st['min_time'] = min(st['min_time'], dur)
                st['max_time'] = max(st['max_time'], dur)
                parent_id = None
                for parent_fid, _, parent_is_user in reversed(call_stack):
                    if parent_is_user:
                        parent_id = parent_fid
                        break
                if parent_id:
                    pst = stats.setdefault(parent_id, {
                        'total_time': 0.0,
                        'call_count': 0,
                        'children_time': 0.0,
                        'min_time': float('inf'),
                        'max_time': 0.0
                    })
                    pst['children_time'] += dur
                    ekey = (parent_id, fid)
                    edges[ekey]['call_count'] += 1
                    edges[ekey]['total_time'] += dur

    system_info = get_system_info()
    sampler = threading.Thread(target=system_sampler, daemon=True)
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
    except Exception:
        traceback.print_exc()
        exit_code = 1
    finally:
        end_wall = time.perf_counter()
        end_cpu = time.process_time()
        sys.setprofile(None)
        sampling = False
        sampler.join(timeout=1.0)
        try:
            _, final_mem_peak = tracemalloc.get_traced_memory()
        except Exception:
            final_mem_peak = 0
        tracemalloc.stop()

    sanitized_stats = {}
    for fid, v in stats.items():
        min_time = v.get('min_time', float('inf'))
        if min_time == float('inf') or min_time is None:
            min_time = 0.0
        sanitized_stats[fid] = {
            'total_time': float(v.get('total_time', 0.0)),
            'call_count': int(v.get('call_count', 0)),
            'children_time': float(v.get('children_time', 0.0)),
            'min_time': float(min_time),
            'max_time': float(v.get('max_time', 0.0))
        }

    edge_list = []
    for (p, c), info in edges.items():
        edge_list.append({
            'caller': p,
            'callee': c,
            'call_count': int(info.get('call_count', 0)),
            'total_time': float(info.get('total_time', 0.0))
        })

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

    final_gc_stats = []
    try:
        final_gc_stats = gc.get_stats()
    except Exception:
        final_gc_stats = []

    final_gc_counts = ()
    try:
        final_gc_counts = gc.get_count()
    except Exception:
        final_gc_counts = ()

    total_gc_collections = sum(gen.get('collections', 0) for gen in final_gc_stats) if final_gc_stats else 0

    raw_report = {
        'meta': {
            'language': 'python',
            'command': [target_path] + target_args,
            'wall_time_s': end_wall - start_wall,
            'cpu_time_s': end_cpu - start_cpu,
            'exit_code': exit_code,
            'timestamp': time.time(),
            'system_info': system_info
        },
        'stats': sanitized_stats,
        'edges': edge_list,
        'memory_samples': mem_samples,
        'cpu_samples': cpu_samples,
        'gc_samples': gc_stats,
        'peak_tracemalloc': final_mem_peak,
        'peak_rss': peak_rss,
        'final_gc_stats': final_gc_stats,
        'final_gc_counts': list(final_gc_counts) if isinstance(final_gc_counts, tuple) else final_gc_counts,
        'total_gc_collections': total_gc_collections
    }

    os.makedirs(report_dir, exist_ok=True)
    raw_path = os.path.join(report_dir, "raw_profile.json")
    with open(raw_path, "w") as f:
        json.dump(raw_report, f, indent=2)

    print(f"Profiler finished. Raw profile: {raw_path}")

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
