#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    
    if not line:
        continue
        
    # Just pass the output from reducer1 to mapper2
    print(line)