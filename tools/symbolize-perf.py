#!/usr/bin/env python

import subprocess
import re
import sys

STACK_LINE = re.compile('^\t *([a-f0-9]*) (\S+) \((.*)\)$')
OUT_FMT = "\t %x %s (%s)"

SYMBOLIZE = ['llvm-symbolizer-3.5', '-demangle=true']

class Symbolicator(object):

  def __init__(self):
    self._child = subprocess.Popen(SYMBOLIZE, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    self._cache = {}
    self.hits = 0
    self.misses = 0

  def symbolize(self, obj, addr):
    key = (obj, addr)
    if key not in self._cache:
        self.misses += 1
        self._child.stdin.write("%s %#x\n" % (obj, addr))
        lines = []
        while self._child.poll() is None:
          line = self._child.stdout.readline()
          if len(line.rstrip()) == 0:
            break
          symb = line.strip()

          line = self._child.stdout.readline()
          if len(line.rstrip()) == 0:
            break
          posn = line.strip()
          lines.append((symb, posn))
        self._cache[key] = lines
        return lines
    else:
        self.hits += 1
        return self._cache[key]

def main(instr, out):
  symbolicator = Symbolicator()

  for line in instr:
    m = STACK_LINE.match(line)
    if m:
      addr, symb, obj = m.groups()
      addr = int(addr, 16)
      #out.write(repr(dict(addr=addr, symb=symb, obj=obj)))
      #out.write('\n')
      for (symbol, posn) in symbolicator.symbolize(obj, addr):
	if symbol != '??':
	  symb = symbol
	out.write(OUT_FMT % (addr, symb, obj))
	out.write('\n')
    else:
      out.write(line)

  total = symbolicator.hits + symbolicator.misses
  print >> sys.stderr, "hits: %2.2f%% of %d" % ((100 * symbolicator.hits / float(total)), total)
      
if __name__ == '__main__':
  import sys
  main(sys.stdin, sys.stdout)
