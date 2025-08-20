import sys, os, time, tracemalloc, json, threading
from collections import defaultdict, deque
import traceback

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

    html = html_template(json.dumps(report))
    with open(os.path.join(report_dir, "report.html"), "w", encoding='utf-8') as f:
        f.write(html)

    print(f"Profiler finished. Reports: {report_path}, {os.path.join(report_dir, 'report.html')}")

def html_template(json_text):
    return f"""<!doctype html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>Python Profile Report</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{ 
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; 
      margin: 0; padding: 20px; 
      background: #f8fafc; 
      color: #1e293b;
      line-height: 1.5;
    }}
    .container {{ max-width: 1400px; margin: 0 auto; }}
    header {{ margin-bottom: 32px; }}
    h1 {{ 
      font-size: 2rem; font-weight: 700; margin: 0 0 8px 0; 
      background: linear-gradient(135deg, #3b82f6, #8b5cf6);
      -webkit-background-clip: text; background-clip: text;
      -webkit-text-fill-color: transparent;
    }}
    .subtitle {{ color: #64748b; font-size: 1.1rem; margin-bottom: 16px; }}
    .grid {{ display: grid; grid-template-columns: 2fr 1fr; gap: 24px; }}
    .card {{ 
      background: white; 
      border-radius: 12px; 
      box-shadow: 0 1px 3px rgba(0,0,0,0.1), 0 1px 2px rgba(0,0,0,0.06);
      overflow: hidden;
    }}
    .card-header {{ 
      background: #f1f5f9; 
      padding: 16px 20px; 
      border-bottom: 1px solid #e2e8f0;
    }}
    .card-title {{ 
      font-size: 1.25rem; font-weight: 600; margin: 0; 
      display: flex; align-items: center; gap: 8px;
    }}
    .card-content {{ padding: 0; }}
    .icon {{ width: 20px; height: 20px; }}

    table {{ width: 100%; border-collapse: collapse; }}
    thead th {{ 
      background: #f8fafc; 
      padding: 12px 16px; 
      text-align: left; 
      font-weight: 600; 
      border-bottom: 2px solid #e2e8f0;
      cursor: pointer;
      transition: background-color 0.2s;
      user-select: none;
    }}
    thead th:hover {{ background: #f1f5f9; }}
    tbody td {{ 
      padding: 12px 16px; 
      border-bottom: 1px solid #f1f5f9;
      vertical-align: middle;
    }}
    tbody tr:hover {{ background: #f8fafc; }}

    .function-name {{ 
      font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace; 
      font-size: 0.875rem; 
      background: #f1f5f9; 
      padding: 4px 8px; 
      border-radius: 6px; 
      display: inline-block;
      max-width: 400px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}

    .time-bar {{ 
      height: 20px; 
      background: linear-gradient(90deg, #3b82f6, #1d4ed8); 
      border-radius: 4px; 
      display: inline-block;
      min-width: 2px;
      position: relative;
      overflow: hidden;
    }}
    .time-bar::after {{
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0; bottom: 0;
      background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
      animation: shimmer 2s infinite;
    }}
    @keyframes shimmer {{
      0% {{ transform: translateX(-100%); }}
      100% {{ transform: translateX(100%); }}
    }}

    .metric {{ text-align: center; margin-bottom: 16px; }}
    .metric-value {{ 
      font-size: 1.5rem; font-weight: 700; 
      color: #3b82f6; 
      display: block; 
    }}
    .metric-label {{ 
      color: #64748b; 
      font-size: 0.875rem; 
      text-transform: uppercase; 
      letter-spacing: 0.5px;
    }}

    .memory-chart {{ 
      height: 200px; 
      padding: 16px; 
      overflow-y: auto; 
      background: #fafafa;
    }}
    .memory-sample {{ 
      display: flex; 
      align-items: center; 
      margin-bottom: 4px; 
      font-size: 0.875rem;
    }}
    .memory-time {{ 
      width: 80px; 
      color: #64748b; 
      font-family: monospace;
    }}
    .memory-bar-container {{ 
      flex: 1; 
      background: #e2e8f0; 
      height: 16px; 
      border-radius: 8px; 
      overflow: hidden; 
      margin: 0 12px;
    }}
    .memory-bar {{ 
      height: 100%; 
      background: linear-gradient(90deg, #10b981, #059669); 
      border-radius: 8px;
      transition: width 0.3s ease;
    }}
    .memory-value {{ 
      width: 80px; 
      text-align: right; 
      color: #374151; 
      font-weight: 500;
    }}

    .meta-info {{ 
      padding: 16px; 
      background: #f8fafc; 
      font-family: monospace; 
      font-size: 0.875rem; 
      line-height: 1.6;
    }}
    .meta-row {{ 
      display: flex; 
      justify-content: space-between; 
      margin-bottom: 4px;
    }}
    .meta-label {{ color: #64748b; }}
    .meta-value {{ 
      color: #1e293b; 
      font-weight: 500;
    }}

    .sort-indicator {{ 
      opacity: 0.5; 
      margin-left: 4px; 
      font-size: 0.75rem;
    }}
    .sort-indicator.active {{ opacity: 1; }}

    .empty-state {{ 
      text-align: center; 
      color: #64748b; 
      padding: 40px; 
    }}

    @media (max-width: 768px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .container {{ padding: 16px; }}
      h1 {{ font-size: 1.5rem; }}
    }}
    </style>
    </head>
    <body>
    <div class="container">
      <header>
        <h1>Python Profile Report</h1>
        <div class="subtitle">Performance analysis of your Python application</div>
      </header>

      <div class="grid">
        <div class="card">
          <div class="card-header">
            <h2 class="card-title">
              Function Performance
            </h2>
          </div>
          <div class="card-content">
            <table id="func-table">
              <thead>
                <tr>
                  <th data-key="total_time">
                    Total Time <span class="sort-indicator active">v</span>
                  </th>
                  <th data-key="exclusive_time">
                    Exclusive Time <span class="sort-indicator">v</span>
                  </th>
                  <th data-key="call_count">
                    Calls <span class="sort-indicator">v</span>
                  </th>
                  <th>Function</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>

        <div>
          <div class="card" style="margin-bottom: 24px;">
            <div class="card-header">
              <h2 class="card-title">
                Overview
              </h2>
            </div>
            <div class="card-content">
              <div style="padding: 16px;">
                <div class="metric">
                  <span class="metric-value" id="total-time">-</span>
                  <span class="metric-label">Total Runtime</span>
                </div>
                <div class="metric">
                  <span class="metric-value" id="function-count">-</span>
                  <span class="metric-label">Functions Profiled</span>
                </div>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-header">
              <h2 class="card-title">
                Memory Usage
              </h2>
            </div>
            <div class="card-content">
              <div class="memory-chart" id="memory-chart"></div>
              <div class="meta-info" id="meta-info"></div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <script>
    const report = {json_text};

    // DOM elements
    const tbody = document.querySelector("#func-table tbody");
    const totalTimeEl = document.getElementById("total-time");
    const functionCountEl = document.getElementById("function-count");
    const memoryChart = document.getElementById("memory-chart");
    const metaInfo = document.getElementById("meta-info");

    // State
    let nodes = report.nodes.slice();
    let currentSort = {{ key: 'total_time', desc: true }};

    // Format time in a human readable way
    function formatTime(seconds) {{
      if (seconds < 0.001) return `${{(seconds * 1000000).toFixed(0)}}&micro;s`;
      if (seconds < 1) return `${{(seconds * 1000).toFixed(1)}}ms`;
      return `${{seconds.toFixed(3)}}s`;
    }}

    // Format memory in human readable way
    function formatMemory(bytes) {{
      if (bytes < 1024) return `${{bytes}}B`;
      if (bytes < 1024 * 1024) return `${{(bytes / 1024).toFixed(1)}}KB`;
      return `${{(bytes / (1024 * 1024)).toFixed(1)}}MB`;
    }}

    // Render function table
    function renderTable() {{
      tbody.innerHTML = "";
      if (nodes.length === 0) {{
        tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No functions profiled</td></tr>';
        return;
      }}

      const maxTime = Math.max(...nodes.map(n => n.total_time), 1e-9);
      
      nodes.forEach(n => {{
        const tr = document.createElement("tr");
        const barWidth = Math.max(2, Math.round((n.total_time / maxTime) * 200));
        
        tr.innerHTML = `
          <td>
            <div style="display: flex; align-items: center; gap: 8px;">
              <div class="time-bar" style="width: ${{barWidth}}px;"></div>
              <span>${{formatTime(n.total_time)}}</span>
            </div>
          </td>
          <td>${{formatTime(n.exclusive_time)}}</td>
          <td style="font-weight: 600; color: #3b82f6;">${{n.call_count.toLocaleString()}}</td>
          <td><div class="function-name" title="${{n.id}}">${{n.id}}</div></td>
        `;
        tbody.appendChild(tr);
      }});
    }}

    // Sort functions
    function sortBy(key, desc = false) {{
      currentSort = {{ key, desc }};
      nodes.sort((a, b) => {{
        const aVal = a[key];
        const bVal = b[key];
        if (aVal === bVal) return 0;
        return (aVal < bVal ? -1 : 1) * (desc ? -1 : 1);
      }});
      
      // Update sort indicators
      document.querySelectorAll('.sort-indicator').forEach(ind => ind.classList.remove('active'));
      const activeHeader = document.querySelector(`th[data-key="${{key}}"] .sort-indicator`);
      if (activeHeader) {{
        activeHeader.classList.add('active');
        activeHeader.textContent = desc ? 'v' : '^';
      }}
      
      renderTable();
    }}

    // Set up sorting
    document.querySelectorAll("#func-table th[data-key]").forEach(th => {{
      th.addEventListener("click", () => {{
        const key = th.getAttribute("data-key");
        const desc = currentSort.key === key ? !currentSort.desc : true;
        sortBy(key, desc);
      }});
    }});

    // Render memory chart
    function renderMemoryChart() {{
      const samples = report.memory_samples;
      if (samples.length === 0) {{
        memoryChart.innerHTML = '<div class="empty-state">No memory samples recorded</div>';
        return;
      }}

      const maxPeak = Math.max(...samples.map(s => s.peak));
      const fragment = document.createDocumentFragment();
      
      // Show max 100 samples to avoid overwhelming the display
      const step = Math.max(1, Math.floor(samples.length / 100));
      for (let i = 0; i < samples.length; i += step) {{
        const s = samples[i];
        const div = document.createElement("div");
        div.className = "memory-sample";
        
        const width = Math.max(1, Math.round((s.peak / maxPeak) * 100));
        div.innerHTML = `
          <div class="memory-time">${{s.t.toFixed(2)}}s</div>
          <div class="memory-bar-container">
            <div class="memory-bar" style="width: ${{width}}%"></div>
          </div>
          <div class="memory-value">${{formatMemory(s.peak)}}</div>
        `;
        fragment.appendChild(div);
      }}
      
      memoryChart.appendChild(fragment);
    }}

    // Render meta information
    function renderMeta() {{
      const meta = report.meta;
      totalTimeEl.textContent = formatTime(meta.wall_time_s);
      functionCountEl.textContent = nodes.length.toLocaleString();
      
      metaInfo.innerHTML = `
        <div class="meta-row">
          <span class="meta-label">Wall Time:</span>
          <span class="meta-value">${{formatTime(meta.wall_time_s)}}</span>
        </div>
        <div class="meta-row">
          <span class="meta-label">CPU Time:</span>
          <span class="meta-value">${{formatTime(meta.cpu_time_s)}}</span>
        </div>
        <div class="meta-row">
          <span class="meta-label">Exit Code:</span>
          <span class="meta-value">${{meta.exit_code}}</span>
        </div>
        ${{report.peak_rss ? `
        <div class="meta-row">
          <span class="meta-label">Peak RSS:</span>
          <span class="meta-value">${{formatMemory(report.peak_rss * 1024)}}</span>
        </div>
        ` : ''}}
      `;
    }}

    // Initialize
    renderTable();
    renderMemoryChart();
    renderMeta();
    </script>
    </body>
    </html>
    """

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
