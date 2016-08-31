#!/usr/bin/env python

import graphviz
import json
import collections
import sys
import base64

b32e = base64.b32encode

def trunc(s):
    if len(s) > 20:
        return s[:18] + '...'
    else:
        return s

def parse(f):
    proc_tls = collections.defaultdict(dict)
    messages = []
    ts = set()
    processes = set()
    node_crashes = {}
    last_deliveries = {}

    records = json.load(f)
    for data in records:
        if 'ProcessState' in data:
            s = data['ProcessState'];
            pid = str(s['process'])
            time = tuple(s['time'])
            proc_tls[pid][time] = s['state']
            ts.add(time)
            processes.add(pid)

        elif 'Committed' in data:
            s = data['Committed']
            pid = str(s['process'])
            time = tuple(s['time'])
            offset = str(dict(committed_to=s['offset']))
            proc_tls[pid][time] = offset
            ts.add(time)
            processes.add(pid)

        elif 'MessageRecv' in data:
            r = data['MessageRecv']
            dst = str(r['dst']);
            messages.append(r)
            processes.add(str(r['src']))
            processes.add(dst)
            ts.add(tuple(r['sent']))
            recv  = tuple(r['recv'])
            ts.add(recv)
            last = last_deliveries.get(dst, (0,));
            last_deliveries[dst] = recv if last < recv else last

        elif 'NodeCrashed' in data:
            c = data['NodeCrashed']
            proc = str(c['process'])
            t = tuple(c['time'])
            processes.add(proc)
            ts.add(t)
            node_crashes[proc] = t
        else:
            print >> sys.stderr, "unrecognised type: %r" % (data.keys(),)

    graph = graphviz.Digraph()# rankdir="TD", splines="line");

    nodes = graphviz.Digraph('cluster_proc_tls', graph_attr=dict(label=''));
    for p in sorted(processes):
        nid = b32e(str(p))
        nodes.node("proc_%s" % nid, label=str(p), group=nid)

    graph.subgraph(nodes)

    prev_tid = None
    prevs = {}
    print >> sys.stderr, repr(('ts', sorted(ts)))
    for t in sorted(ts):
        tid = b32e(str(t))
        for p in processes:
            tl = proc_tls[p]
            pid = b32e(str(p))
            crash_time = node_crashes.get(p, (sys.maxint,))
            last_delivery = last_deliveries.get(p, (0,))
            # print >> sys.stderr, (prev_tid, tid, crash_time)
            if t == crash_time:
                graph.node('state_%s_%s' % (pid, tid), label = 'CRASHED', color='red', shape='box', group=pid)
            elif t in tl and tl[t] != prevs.get(p, None):
                graph.node('state_%s_%s' % (pid, tid), label = str(t), group=pid, tooltip=tl[t])
                prevs[p] = tl[t]
            elif t >= crash_time and t <= last_delivery:
                graph.node('state_%s_%s' % (pid, tid), group=pid, shape="point", color="red")
            elif t < crash_time:
                graph.node('state_%s_%s' % (pid, tid), group=pid, shape="point")

            if t > last_delivery and t > crash_time:
                pass
            elif prev_tid:
                graph.edge('state_%s_%s' % (pid, prev_tid), 'state_%s_%s' % (pid, tid), weight='2', arrowhead="none", color="gray75")
            else:
                graph.edge('proc_%s' % (pid,), 'state_%s_%s' % (pid, tid), weight='2', arrowhead="none", color="gray75")
        prev_tid = tid

    for m in messages:
        dst = str(m['dst'])
        recv = tuple(m['recv'])
        crash_time = node_crashes.get(dst, (sys.maxint,))
        src = 'state_%s_%s' % (b32e(str(m['src'])), b32e(str(tuple(m['sent']))))
        dst = 'state_%s_%s' % (b32e(dst), b32e(str(recv)))
        crashedp = recv > crash_time
        print >> sys.stderr, repr(dict(dst=dst, recv=recv, crash_time=crash_time, crashedp=crashedp))
        graph.edge(src, dst, constraint="false",
            label=trunc(str(m['data'])), labeltooltip=str(m['data']), labelURL='#',
            headlabel=repr(m['recv']), taillabel=repr(m['sent']),
            color=('red' if crashedp else 'black')
            )

    print graph.source

if __name__ == '__main__':
    import sys
    parse(file(sys.argv[1]))
