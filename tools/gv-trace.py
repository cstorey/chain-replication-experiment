#!/usr/bin/env python

import graphviz
import json
import collections
import sys
import base64

b32e = base64.b32encode
b32e = str

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

    records = json.load(f)
    for data in records:
        if 'ProcessState' in data:
            s = data['ProcessState'];
            pid = s['process']
            time = tuple(s['time'])
            proc_tls[pid][time] = s['state']
            ts.add(time)
            processes.add(pid)

        elif 'MessageRecv' in data:
            r = data['MessageRecv']
            messages.append(r)
            processes.add(r['src'])
            processes.add(r['dst'])
            ts.add(tuple(r['sent']))
            ts.add(tuple(r['recv']))
        elif 'NodeCrashed' in data:
            c = data['NodeCrashed']
            proc = c['process'];
            t = tuple(c['time'])
            processes.add(proc)
            ts.add(t)
            node_crashes[proc] = t
        else:
            print >> sys.stderr, "unrecognised type: %(type)s" % data

    graph = graphviz.Digraph()# rankdir="TD", splines="line");
   
    nodes = graphviz.Digraph('cluster_proc_tls', graph_attr=dict(label=''));
    for p in sorted(processes):
        nid = b32e(str(p))
        nodes.node("proc_%s" % nid, label=str(p), group=nid)

    graph.subgraph(nodes)
 
    prev_tid = None
    prevs = {}
    for t in sorted(ts):
        tid = b32e(t)
        for p in processes:
            tl = proc_tls[p]
            pid = b32e(str(p))
            crash_time = node_crashes.get(p, (sys.maxint,))
            # print >> sys.stderr, (prev_tid, tid, crash_time)
            if t == crash_time:
                graph.node('state_%s_%s' % (pid, tid), label = 'CRASHED', color='red', shape='box', group=pid)
            elif t in tl and tl[t] != prevs.get(p, None):
                graph.node('state_%s_%s' % (pid, tid), label = str(t), group=pid, tooltip=tl[t])
                prevs[p] = tl[t]
            elif t < crash_time:
                graph.node('state_%s_%s' % (pid, tid), group=pid, shape="point")

            if t > crash_time:
                pass
            elif prev_tid:
                graph.edge('state_%s_%s' % (pid, prev_tid), 'state_%s_%s' % (pid, tid), weight='2', arrowhead="none", color="gray75")
            else:
                graph.edge('proc_%s' % (pid,), 'state_%s_%s' % (pid, tid), weight='2', arrowhead="none", color="gray75")
        prev_tid = tid

    for m in messages:
	src = 'state_%s_%s' % (b32e(m['src']), b32e(tuple(m['sent'])))
	dst = 'state_%s_%s' % (b32e(m['dst']), b32e(tuple(m['recv'])))
        graph.edge(src, dst, constraint="false",
	    label=trunc(str(m['data'])), labeltooltip=str(m['data']), labelURL='#',
	    headlabel=repr(m['recv']), taillabel=repr(m['sent']))
        



    print graph.source



if __name__ == '__main__':

    import sys
    parse(file(sys.argv[1]))
