# -*- coding: utf-8 -*-
with open(r'd:\Antigravity\Stock Block Flow\index.html', 'r', encoding='utf-8') as f:
    html = f.read()

template = """# -*- coding: utf-8 -*-
import os

html_content = \"\"\"__HTML_PLACEHOLDER__\"\"\"

# Write the index.html with UTF-8
with open('d:\\\\Antigravity\\\\Stock Block Flow\\\\index.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print("[SUCCESS] HTML file written correctly via Python.")
"""

content = template.replace("__HTML_PLACEHOLDER__", html)

with open(r'd:\Antigravity\Stock Block Flow\write_index.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Sync completed!")
