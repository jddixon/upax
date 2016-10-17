#!/usr/bin/env python3

import mistune

md = "[link to Consensus](https://en.wikipedia.org/wiki/Consensus_%28computer_science%29)"

output = mistune.markdown(md)
print(output)
