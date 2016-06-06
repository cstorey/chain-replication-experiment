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

    for line in f:
        data = json.loads(line)

        if data['type'] == 'state':
            pid = data['process']
            time = tuple(data['time'])
            proc_tls[pid][time] = data['state']
            ts.add(time)
            processes.add(pid)

        elif data['type'] == 'recv':
            messages.append(data)
            processes.add(data['src'])
            processes.add(data['dst'])
            ts.add(tuple(data['sent']))
            ts.add(tuple(data['recv']))
        elif data['type'] == 'node_crash':
            proc = data['process'];
            t = tuple(data['time'])
            processes.add(proc)
            ts.add(t)
            node_crashes[proc] = t
        else:
            print >> sys.stderr, "unrecognised type: %(type)s" % data

    graph = graphviz.Digraph()# rankdir="TD", splines="line");
   
    nodes = graphviz.Digraph('cluster_proc_tls', graph_attr=dict(label=''));
    for p in sorted(processes):
        nid = b32e(p)
        nodes.node("proc_%s" % nid, label=p, group=nid)

    graph.subgraph(nodes)
 
    prev_tid = None
    prevs = {}
    for t in sorted(ts):
        tid = b32e(t)
        for p in processes:
            tl = proc_tls[p]
            pid = b32e(p)
            crash_time = node_crashes.get(p, (sys.maxint,))
            print >> sys.stderr, (prev_tid, tid, crash_time)
            if t == crash_time:
                graph.node('state_%s_%s' % (pid, tid), label = 'CRASHED', color='red', shape='box', group=pid)
            elif t in tl and tl[t] != prevs.get(p, None):
                graph.node('state_%s_%s' % (pid, tid), label = str(t), group=pid, tooltip=tl[t])
                prevs[p] = tl[t]
            else:
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
	    label=trunc(m['data']), labeltooltip=m['data'], labelURL='#',
	    headlabel=repr(m['recv']), taillabel=repr(m['sent']))
        



    print graph.source



if __name__ == '__main__':

    import sys
    parse(file(sys.argv[1]))
